/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.voltdb.exportclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.utils.CoreUtils;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class FirehoseSink {
    private final static FirehoseExportLogger LOG = new FirehoseExportLogger();

    private final List<ListeningExecutorService> m_executors;

    private final String m_streamName;
    private AmazonKinesisFirehoseClient m_client;
    private final int m_concurrentWriters;
    private final AtomicInteger m_backpressureIndication = new AtomicInteger(0);
    private BackOff m_backOff;

    public FirehoseSink(String streamName, AmazonKinesisFirehoseClient client, int concurrentWriters, BackOff backOff) {
        ImmutableList.Builder<ListeningExecutorService> lbldr = ImmutableList.builder();
        for (int i = 0; i < concurrentWriters; ++i) {
            String threadName = "Firehose Deliver Stream " + streamName + " Sink Writer " + i;
            lbldr.add(CoreUtils.getListeningSingleThreadExecutor(threadName, CoreUtils.MEDIUM_STACK_SIZE));
        }
        m_executors = lbldr.build();
        m_streamName = streamName;
        m_client = client;
        m_concurrentWriters = concurrentWriters;
        m_backOff = backOff;
    }

    ListenableFuture<?> asWriteTask(List<Record> recordsList) {
        final int hashed = ThreadLocalRandom.current().nextInt(m_concurrentWriters);
        if (m_executors.get(hashed).isShutdown()) {
            return Futures.immediateFailedFuture(new FirehoseExportException("hive sink executor is shut down"));
        }
        return m_executors.get(hashed).submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PutRecordBatchRequest batchRequest = new PutRecordBatchRequest().withDeliveryStreamName(m_streamName)
                        .withRecords(recordsList);
                applyBackPressure();
                PutRecordBatchResult res = m_client.putRecordBatch(batchRequest);
                if (res.getFailedPutCount() > 0) {
                    setBackPressure(true);
                    String msg = "%d Firehose records failed";
                    LOG.warn(msg, res.getFailedPutCount());
                    throw new FirehoseExportException(msg, res.getFailedPutCount());
                }
                setBackPressure(false);
                return null;
            }
        });
    }

    public void write(Queue<List<Record>> records) {
        List<ListenableFuture<?>> tasks = new ArrayList<>();
        for (List<Record> recordsList : records) {
            tasks.add(asWriteTask(recordsList));
        }
        try {
            Futures.allAsList(tasks).get();
        } catch (InterruptedException e) {
            String msg = "Interrupted write for message %s";
            LOG.error(msg, e, records);
            throw new FirehoseExportException(msg, e, records);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof FirehoseExportException) {
                throw (FirehoseExportException) e.getCause();
            }
            String msg = "Fault on write for message %s";
            LOG.error(msg, e, records);
            throw new FirehoseExportException(msg, e, records);
        }
    }

    private boolean setBackPressure(boolean b) {
        int prev = m_backpressureIndication.get();
        int delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
        int next = prev + delta;
        while (next >= 0 && !m_backpressureIndication.compareAndSet(prev, next)) {
            prev = m_backpressureIndication.get();
            delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
            next = prev + delta;
        }
        return b;
    }

    private void applyBackPressure() {
        int sleep = m_backOff.backoff(m_backpressureIndication.get());
        LOG.warn("Sleep for back pressure for %d ms", sleep);
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            LOG.debug("Sleep for back pressure interrupted", e);
        }
    }
}

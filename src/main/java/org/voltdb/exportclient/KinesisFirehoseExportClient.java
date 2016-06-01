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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.decode.CSVStringDecoder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class KinesisFirehoseExportClient extends ExportClientBase {
    private static final ExportClientLogger LOG = new ExportClientLogger();

    private Region m_region;
    private String m_streamName;
    private String m_accessKey;
    private String m_secretKey;
    private TimeZone m_timeZone;
    private String m_recordSeparator;
    private int m_backOffCap;
    private int m_backOffBase;
    private String m_backOffStrategy;
    private final AtomicInteger m_backpressureIndication = new AtomicInteger(0);

    public static final String ROW_LENGTH_LIMIT = "row.length.limit";
    public static final String RECORD_SEPARATOR = "record.separator";
    public static final String BACKOFF_CAP = "backoff.cap.exponent";
    public static final String BACKOFF_BASE = "backoff.base";
    public static final String BACKOFF_STRATEGY = "backoff.strategy";

    @Override
    public void configure(Properties config) throws Exception
    {
        String regionName = config.getProperty("region","").trim();
        if (regionName.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide a region");
        }
        m_region = RegionUtils.getRegion(regionName);

        m_streamName = config.getProperty("stream.name","").trim();
        if (m_streamName.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide a stream.name");
        }

        m_accessKey = config.getProperty("access.key","").trim();
        if (m_accessKey.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide an access.key");
        }
        m_secretKey = config.getProperty("secret.key","").trim();
        if (m_secretKey.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide a secret.key");
        }

        m_timeZone = TimeZone.getTimeZone(config.getProperty("timezone", VoltDB.REAL_DEFAULT_TIMEZONE.getID()));


        m_recordSeparator = config.getProperty(RECORD_SEPARATOR,"\n");

        config.setProperty(ROW_LENGTH_LIMIT,
                config.getProperty(ROW_LENGTH_LIMIT,Integer.toString(1024000 - m_recordSeparator.length())));

        m_backOffCap = Integer.parseInt(config.getProperty(BACKOFF_CAP,"1000"));
        m_backOffBase = Integer.parseInt(config.getProperty(BACKOFF_BASE,"64"));
        m_backOffStrategy = config.getProperty(BACKOFF_BASE,"full");
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source)
    {
        return new KinesisFirehoseExportDecoder(source);
    }

    class KinesisFirehoseExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;
        private final CSVStringDecoder m_decoder;

        private boolean m_primed = false;
        private Queue<List<Record>> m_records;
        private List<Record> currentBatch;
        private int m_currentBatchSize;
        private AmazonKinesisFirehoseClient m_firehoseClient;
        private boolean allowBackPressure;

        // minimal interval between each putRecordsBatch api call;
        // based on the limit
        // for small records (row length < 1KB): records/s is the bottleneck
        // for large records (row length > 1KB): data throughput is the bottleneck
        /*
        private int lowWaterMark = 50; // tuned base on orignal limit + small record workload        ;
        private int highWaterMark = 150;
        final private int maxSleepTime = 1000;
        final private int incInterval = 100;
        final private int decInterval = 10;
         */
        public static final int BATCH_NUMBER_LIMIT = 500;
        public static final int BATCH_SIZE_LIMIT = 4*1024*1024;

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public KinesisFirehoseExportDecoder(AdvertisedDataSource source)
        {
            super(source);

            CSVStringDecoder.Builder builder = CSVStringDecoder.builder();
            builder
                .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                .timeZone(m_timeZone)
                .columnNames(source.columnNames)
                .columnTypes(source.columnTypes)
            ;
            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "Kinesis Firehose Export decoder for partition " + source.partitionId
                    + " table " + source.tableName
                    + " generation " + source.m_generation, CoreUtils.MEDIUM_STACK_SIZE);
            m_decoder = builder.build();
            allowBackPressure = true;
        }

        private void validateStream() throws RestartBlockException, InterruptedException {
            DescribeDeliveryStreamRequest describeHoseRequest = new DescribeDeliveryStreamRequest().
                    withDeliveryStreamName(m_streamName);
            DescribeDeliveryStreamResult  describeHoseResult = null;
            String status = "UNDEFINED";
            describeHoseResult = m_firehoseClient.describeDeliveryStream(describeHoseRequest);
            status = describeHoseResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
            if("ACTIVE".equalsIgnoreCase(status)){
                return;
            }
            else if("CREATING".equalsIgnoreCase(status)){
                Thread.sleep(5000);
                validateStream();
            }
            else {
                LOG.error("Cannot use stream %s, responded with %s", m_streamName, status);
                throw new RestartBlockException(true);
            }
        }

        final void checkOnFirstRow() throws RestartBlockException {
            if (!m_primed) try {
                m_firehoseClient = new AmazonKinesisFirehoseClient(
                        new BasicAWSCredentials(m_accessKey, m_secretKey));
                m_firehoseClient.setRegion(m_region);
                validateStream();
            } catch (AmazonServiceException | InterruptedException e) {
                LOG.error("Unable to instantiate a Amazon Kinesis Firehose client", e);
                throw new RestartBlockException("Unable to instantiate a Amazon Kinesis Firehose client", e, true);
            }
            m_primed = true;
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
            if (!m_primed) checkOnFirstRow();
            Record record = new Record();
            try {
                final ExportRowData rd = decodeRow(rowData);
                String decoded = m_decoder.decode(null, rd.values) + m_recordSeparator; // add a record separator ;
                record.withData(ByteBuffer.wrap(decoded.getBytes(StandardCharsets.UTF_8)));
            } catch(IOException e) {
                LOG.error("Failed to build record", e);
                throw new RestartBlockException("Failed to build record", e, true);
            }
            // PutRecordBatchRequest can not contain more than 500 records
            // And up to a limit of 4 MB for the entire request
            if (((m_currentBatchSize + rowSize) > BATCH_SIZE_LIMIT) || (currentBatch.size() >= BATCH_NUMBER_LIMIT)) {
                // roll to next batch
                m_records.add(currentBatch);
                m_currentBatchSize = 0;
                currentBatch = new LinkedList<Record>();
            }
            currentBatch.add(record);
            m_currentBatchSize += rowSize;
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source)
        {
            if (m_firehoseClient != null) m_firehoseClient.shutdown();
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void onBlockStart() throws RestartBlockException
        {
            if (!m_primed) checkOnFirstRow();
            m_records = new LinkedList<List<Record>>();
            m_currentBatchSize = 0;
            currentBatch = new LinkedList<Record>();
        }

        @Override
        public void onBlockCompletion() throws RestartBlockException {
            // add last batch
            if (!currentBatch.isEmpty()) {
                // roll to next batch
                m_records.add(currentBatch);
                m_currentBatchSize = 0;
                currentBatch = new LinkedList<Record>();
            }
            try {
                List<Record> recordsList;
                BackOff backOff;
                switch (m_backOffStrategy) {
                    case "full":
                        backOff = new ExpoBackOffFullJitter(m_backOffBase, m_backOffCap);
                        break;
                    case "equal":
                        backOff = new ExpoBackOffEqualJitter(m_backOffBase, m_backOffCap);
                        break;
                    default:
                        backOff = new ExpoBackOffDecor(m_backOffBase, m_backOffCap);
                        break;
                }

                while (!m_records.isEmpty()) {
                    recordsList = m_records.poll();
                    PutRecordBatchRequest batchRequest = new PutRecordBatchRequest().withDeliveryStreamName(
                            m_streamName).withRecords(recordsList);
                    int trial = 3;
                    boolean retry = true;
                    while (trial > 0 && retry) {
                        applyBackPressure(allowBackPressure, backOff);
                        PutRecordBatchResult res = m_firehoseClient.putRecordBatch(batchRequest);
                        if (res.getFailedPutCount() > 0) {
                            int i = 0;
                            for (PutRecordBatchResponseEntry entry : res.getRequestResponses()) {
                                if (entry.getErrorMessage() != null) {
                                    LOG.warn("Record failed with response: %s, Error Code: %s. ", entry.getErrorMessage(), entry.getErrorCode());
                                    if (!entry.getErrorCode().equals("ServiceUnavailableException")) {
                                        throw new RestartBlockException(true);
                                    }
                                    i++;
                                } else {
                                    recordsList.remove(i);
                                }
                            }
                            setBackPressure(true);
                            batchRequest = new PutRecordBatchRequest().withDeliveryStreamName(
                                    m_streamName).withRecords(recordsList);
                        } else {
                            retry = false;
                            setBackPressure(false);
                        }
                        trial--;
                    }

                    if (trial == 0 && retry) {
                        throw new RestartBlockException(true);
                    }
                }
            } catch (ResourceNotFoundException | InvalidArgumentException | ServiceUnavailableException e) {
                LOG.error("Failed to send record batch", e);
                throw new RestartBlockException("Failed to send record batch", e, true);
            }
        }

        private boolean setBackPressure(boolean b) {
            int prev = m_backpressureIndication.get();
            int delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
            int next = prev + delta;
            while (next >= 0 && (1 << next <= m_backOffCap) && !m_backpressureIndication.compareAndSet(prev, next)) {
                prev = m_backpressureIndication.get();
                delta = b ? 1 : -(prev > 1 ? prev >> 1 : 1);
                next = prev + delta;
            }
            return b;
        }

        private void applyBackPressure(final boolean allowBackPressure, BackOff backOff) {
            if (!allowBackPressure) {
                return;
            }
            int sleep = backOff.backoff(m_backpressureIndication.get());
            LOG.warn("Sleep for back pressure for %d ms", sleep);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                LOG.debug("Sleep for back pressure interrupted", e);
            }
        }
    }

    abstract class BackOff {
        int base;
        int cap;
        Random r;

        public BackOff(int base, int cap) {
            this.base = base;
            this.cap = cap;
            r = new Random();
        }

        public int expo(int n) {
            return Math.min(cap, base * (1<<n));
        }

        abstract public int backoff(int n);
    }

    class ExpoBackOffFullJitter extends BackOff {

        public ExpoBackOffFullJitter(int base, int cap) {
            super(base, cap);
        }

        @Override
        public int backoff(int n) {
            return r.nextInt(expo(n));
        }
    }

    class ExpoBackOffEqualJitter extends BackOff {

        public ExpoBackOffEqualJitter(int base, int cap) {
            super(base, cap);
        }

        @Override
        public int backoff(int n) {
            int v = expo(n) / 2 ;
            return v + r.nextInt(v);
        }
    }

    class ExpoBackOffDecor extends BackOff {
        int sleep;
        public ExpoBackOffDecor(int base, int cap) {
            super(base, cap);
            sleep = base;
        }

        @Override
        public int backoff(int n) {
            sleep = n > 0 ? Math.min(cap, r.nextInt(sleep * 3 - base) + base) : base;
            return sleep;
        }
    }
}

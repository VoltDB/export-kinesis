/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.voltcore.utils.CoreUtils;

import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.decode.v2.CSVStringDecoder;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class KinesisFirehoseExportClient extends ExportClientBase {
    private static final FirehoseExportLogger LOG = new FirehoseExportLogger();

    private Region m_region;
    private String m_streamName;
    private String m_accessKey;
    private String m_secretKey;
    private TimeZone m_timeZone;
    private AmazonKinesisFirehoseClient m_firehoseClient;
    private FirehoseSink m_sink;
    private String m_recordSeparator;


    private int m_backOffCap;
    private int m_backOffBase;
    private int m_streamLimit;
    private int m_concurrentWriter;
    private String m_backOffStrategy;
    private BackOff m_backOff;
    private boolean m_batchMode;
    private int m_batchSize;

    public static final String ROW_LENGTH_LIMIT = "row.length.limit";
    public static final String RECORD_SEPARATOR = "record.separator";

    public static final String BACKOFF_CAP = "backoff.cap";
    public static final String STREAM_LIMIT = "stream.limit";
    public static final String BACKOFF_TYPE = "backoff.type";
    public static final String CONCURRENT_WRITER = "concurrent.writers";
    public static final String BATCH_MODE = "batch.mode";
    public static final String BATCH_SIZE = "batch.size";

    public static final int BATCH_NUMBER_LIMIT = 500;
    public static final int BATCH_SIZE_LIMIT = 4*1024*1024;

    @Override
    public void configure(Properties config) throws Exception {
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
        // minimal interval between each putRecordsBatch api call;
        // for small records (row length < 1KB): record/s is the bottleneck
        // for large records (row length > 1KB): data throughput is the bottleneck
        // for original limit, (5000 records/s  divide by 500 records per call = 10 calls)
        // interval is 1000 ms / 10 = 100 ms
        m_streamLimit = Integer.parseInt(config.getProperty(STREAM_LIMIT,"5000"));
        m_backOffBase = Math.max(2, 1000 / (m_streamLimit/BATCH_NUMBER_LIMIT));

        // concurrent aws client = number of export table to this stream * number of voltdb partition
        m_concurrentWriter = Integer.parseInt(config.getProperty(CONCURRENT_WRITER,"0"));
        m_backOffStrategy = config.getProperty(BACKOFF_TYPE,"equal");

        m_firehoseClient = new AmazonKinesisFirehoseClient(new BasicAWSCredentials(m_accessKey, m_secretKey));
        m_firehoseClient.setRegion(m_region);
        m_backOff = BackOffFactory.getBackOff(m_backOffStrategy, m_backOffBase, m_backOffCap);
        m_sink = new FirehoseSink(m_streamName,m_firehoseClient, m_concurrentWriter, m_backOff);
        m_batchMode = Boolean.parseBoolean(config.getProperty(BATCH_MODE, "true"));
        m_batchSize = Math.min(BATCH_NUMBER_LIMIT, Integer.parseInt(config.getProperty(BATCH_SIZE,"200")));
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new KinesisFirehoseExportDecoder(source);
    }

    class KinesisFirehoseExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;
        private final CSVStringDecoder m_decoder;

        private boolean m_primed = false;
        private Queue<List<Record>> m_records;
        private List<Record> currentBatch;
        private int m_currentBatchSize;

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public KinesisFirehoseExportDecoder(AdvertisedDataSource source) {
            super(source);

            CSVStringDecoder.Builder builder = CSVStringDecoder.builder();
            builder
                .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                .timeZone(m_timeZone);
            // TODO: currently thread name includes table name fetched from ADS name
            // to not include thread name - though having thread name is useful in
            // debugging. An alternative is postpond thread creation to later during
            // onBlockStart() or processRow()
            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "Kinesis Firehose Export decoder for partition " + source.partitionId
                    + " table " + source.tableName
                    + " generation " + source.m_generation, CoreUtils.MEDIUM_STACK_SIZE);
            m_decoder = builder.build();
        }

        private void validateStream() throws RestartBlockException, InterruptedException {
            DescribeDeliveryStreamRequest describeHoseRequest = new DescribeDeliveryStreamRequest().
                    withDeliveryStreamName(m_streamName);
            DescribeDeliveryStreamResult  describeHoseResult = null;
            String status = "UNDEFINED";
            describeHoseResult = m_firehoseClient.describeDeliveryStream(describeHoseRequest);
            status = describeHoseResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
            if ("ACTIVE".equalsIgnoreCase(status)) {
                return;
            }
            else if("CREATING".equalsIgnoreCase(status)) {
                Thread.sleep(5000);
                validateStream();
            }
            else {
                LOG.error("Cannot use stream %s, responded with %s", m_streamName, status);
                throw new RestartBlockException(true);
            }
        }

        final void checkOnFirstRow() throws RestartBlockException {
            if (m_primed) {
                return;
            }

            try {
                validateStream();
            } catch (AmazonServiceException | InterruptedException e) {
                LOG.error("Unable to instantiate a Amazon Kinesis Firehose client", e);
                throw new RestartBlockException("Unable to instantiate a Amazon Kinesis Firehose client", e, true);
            }
            m_primed = true;
        }

        @Override
        public boolean processRow(ExportRowData rowData) throws RestartBlockException {
            if (!m_primed) {
                checkOnFirstRow();
            }

            Record record = new Record();
            String decoded = m_decoder.decode(rowData.generation, rowData.tableName, rowData.types, rowData.names, null, rowData.values)
                    + m_recordSeparator; // add a record separator ;
            final byte[] data = decoded.getBytes(StandardCharsets.UTF_8);
            record.withData(ByteBuffer.wrap(data));

            if (m_batchMode) {
                // PutRecordBatchRequest can not contain more than 500 records
                // And up to a limit of 4 MB for the entire request
                if ((m_currentBatchSize + data.length) > BATCH_SIZE_LIMIT || currentBatch.size() >= m_batchSize) {
                    // roll to next batch
                    m_records.add(currentBatch);
                    m_currentBatchSize = 0;
                    currentBatch = new LinkedList<Record>();
                }
                currentBatch.add(record);
                m_currentBatchSize += data.length;
            }
            else {
                try {
                    m_sink.writeRow(record);
                }
                catch (FirehoseExportException e) {
                    throw new RestartBlockException("firehose write fault", e, true);
                }
                catch (ResourceNotFoundException | InvalidArgumentException | ServiceUnavailableException e) {
                    LOG.error("Failed to send record batch", e);
                    throw new RestartBlockException("Failed to send record batch", e, true);
                }
            }
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source)
        {
            if (m_sink != null) {
                m_sink.shutDown();
            }
            if (m_firehoseClient != null) {
                m_firehoseClient.shutdown();
            }
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            }
            catch (InterruptedException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void onBlockStart(ExportRowData row) throws RestartBlockException {
            if (!m_primed) {
                checkOnFirstRow();
            }
            m_records = new LinkedList<List<Record>>();
            m_currentBatchSize = 0;
            currentBatch = new LinkedList<Record>();
        }

        @Override
        public void onBlockCompletion(ExportRowData row) throws RestartBlockException {

            if(m_batchMode){
                // add last batch
                if (!currentBatch.isEmpty()) {
                    // roll to next batch
                    m_records.add(currentBatch);
                    m_currentBatchSize = 0;
                    currentBatch = new LinkedList<Record>();
                }

                try {
                    if (m_concurrentWriter > 0) {
                        m_sink.write(m_records);
                    }
                    else {
                        m_sink.syncWrite(m_records);
                    }
                }
                catch (FirehoseExportException e) {
                    throw new RestartBlockException("firehose write fault", e, true);
                }
                catch (ResourceNotFoundException | InvalidArgumentException | ServiceUnavailableException e) {
                    LOG.error("Failed to send record batch", e);
                    throw new RestartBlockException("Failed to send record batch", e, true);
                }
            }
        }
    }
}

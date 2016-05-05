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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

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
    public static final String ROW_LENGTH_LIMIT = "row.length.limit";

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

        config.setProperty(ROW_LENGTH_LIMIT,
                config.getProperty(ROW_LENGTH_LIMIT,"1_000_000"));
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
        private ArrayList<Record> m_records;
        private AmazonKinesisFirehoseClient m_firehoseClient;

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
        }

        private void validateStream() throws RestartBlockException, InterruptedException {
            DescribeDeliveryStreamRequest describeHoseRequest = new DescribeDeliveryStreamRequest().
                    withDeliveryStreamName(m_streamName);
            DescribeDeliveryStreamResult  describeHoseResult = null;
            String status = "UNDEFINED";
            describeHoseResult = m_firehoseClient.describeDeliveryStream(describeHoseRequest);
            status = describeHoseResult.getDeliveryStreamDescription().getDeliveryStreamStatus();
            if(status.equalsIgnoreCase("ACTIVE")){
                return;
            }
            else if(status.equalsIgnoreCase("CREATING")){
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
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException
        {
            if (!m_primed) checkOnFirstRow();
            Record record = new Record();
            try {
                final ExportRowData rd = decodeRow(rowData);
                String decoded = m_decoder.decode(null, rd.values);
                record.withData(ByteBuffer.wrap(decoded.getBytes()));
            } catch(IOException e) {
                LOG.error("Failed to build record", e);
                throw new RestartBlockException("Failed to build record", e, true);
            }

            m_records.add(record);
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
            m_records = new ArrayList<Record>();
        }

        @Override
        public void onBlockCompletion() throws RestartBlockException
        {
            try {
                final int recordsSize = m_records.size();

                List<Record> recordsList;
                int rowTracker = 0;
                int sleepTime = 0;

                while (recordsSize > rowTracker) {
                    if (sleepTime > 0)
                        Thread.sleep(sleepTime);
                    // PutRecordBatchRequest can not contain more than 500 records
                    recordsList = m_records.subList(rowTracker,
                            rowTracker = (recordsSize > rowTracker + 500) ? rowTracker + 500 : recordsSize);
                    PutRecordBatchRequest batchRequest = new PutRecordBatchRequest().
                            withDeliveryStreamName(m_streamName).
                            withRecords(recordsList);
                    PutRecordBatchResult res = m_firehoseClient.putRecordBatch(batchRequest);
                    if (res.getFailedPutCount() > 0) {
                        for (PutRecordBatchResponseEntry entry : res.getRequestResponses()) {
                            if (entry.getErrorMessage() != null && !entry.getErrorMessage().contains("Slow down.")) {
                                LOG.error("Record failed with response: %s", entry.getErrorMessage());
                                throw new RestartBlockException(true);
                            }
                        }
                        rowTracker -= recordsList.size();
                        sleepTime = sleepTime == 0 ? 1000 : sleepTime*2;
                    } else
                        sleepTime = sleepTime == 0 ? 0 : sleepTime-10;
                }
            } catch (ResourceNotFoundException | InvalidArgumentException | ServiceUnavailableException | InterruptedException e) {
                LOG.error("Failed to send record batch", e);
                throw new RestartBlockException("Failed to send record batch", e, true);
            }
        }
    }
}

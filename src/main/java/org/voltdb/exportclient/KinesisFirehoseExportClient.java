/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 */

package org.voltdb.exportclient;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;

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
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    String m_streamName = null;
    AmazonKinesisFirehoseClient m_firehoseClient = null;
    boolean m_skipInternal;
    ExportDecoderBase.BinaryEncoding m_binaryEncoding;
    char m_seperator;
    // use thread-local to avoid SimpleDateFormat thread-safety issues
    ThreadLocal<SimpleDateFormat> m_ODBCDateformat;

    @Override
    public void configure(Properties config) throws Exception
    {
        String regionName = config.getProperty("region","").trim();
        if (regionName.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide an region");
        }
        Region region = RegionUtils.getRegion(regionName);

        m_streamName = config.getProperty("stream.name","").trim();
        if (m_streamName.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide a stream.name");
        }

        String accessKey = config.getProperty("access.key","").trim();
        if (accessKey.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide an access.key");
        }
        String secretKey = config.getProperty("secret.key","").trim();
        if (secretKey.isEmpty()) {
            throw new IllegalArgumentException("KinesisFirehoseExportClient: must provide a secret.key");
        }
        m_firehoseClient = new AmazonKinesisFirehoseClient(
                new BasicAWSCredentials(accessKey, secretKey));
        m_firehoseClient.setRegion(region);

        m_skipInternal = Boolean.parseBoolean(config.getProperty("skipinternals", "false"));

        final TimeZone tz = TimeZone.getTimeZone(config.getProperty("timezone", VoltDB.GMT_TIMEZONE.getID()));
        m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
                sdf.setTimeZone(tz);
                return sdf;
            }
        };

        m_binaryEncoding = ExportDecoderBase.BinaryEncoding.valueOf(
                config.getProperty("binaryencoding", "HEX").trim().toUpperCase());

        // Default to CSV if missing
        String type = config.getProperty("type", "csv").trim();
        if (type.equalsIgnoreCase("csv")) {
            m_seperator = ',';
        } else if (type.equalsIgnoreCase("tsv")) {
            m_seperator = '\t';
        } else {
            throw new IllegalArgumentException("Error: type must be one of CSV or TSV");
        }

        validateStream();
        // Kinesis firehose limits record size to 1,000 KB
        setRowLengthLimit(1000000);
    }

    private void validateStream() throws Exception {
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
            throw new Exception("Cannot use stream " + m_streamName + ", responded with " + status);
        }
    }

    @Override
    public void shutdown()
    {
        m_firehoseClient.shutdown();
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source)
    {
        return new KinesisFirehoseExportDecoder(source);
    }

    class KinesisFirehoseExportDecoder extends ExportDecoderBase {
        private final ListeningExecutorService m_es;

        private ArrayList<Record> m_records;

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        public KinesisFirehoseExportDecoder(AdvertisedDataSource source)
        {
            super(source);
            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "Kinesis Firehose Export decoder for partition " + source.partitionId
                    + " table " + source.tableName
                    + " generation " + source.m_generation, CoreUtils.MEDIUM_STACK_SIZE);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException
        {
            StringWriter stringer = new StringWriter();
            CSVWriter csv = new CSVWriter(stringer, m_seperator);
            Record record = new Record();
            try {
                final ExportRowData row = decodeRow(rowData);
                if (!writeRow(row.values, csv, m_skipInternal, m_binaryEncoding, m_ODBCDateformat.get())) {
                    return false;
                }
                csv.flush();

                String data = stringer.toString();
                record.withData(ByteBuffer.wrap(data.getBytes()));
            } catch(IOException e) {
                rateLimitedLogError(m_logger, "Failed to build record: %s", Throwables.getStackTraceAsString(e));
                throw new RestartBlockException(true);
            } finally {
                try { csv.close(); } catch (IOException e) {}
            }

            m_records.add(record);
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source)
        {
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
            m_records = new ArrayList<Record>();
        }

        @Override
        public void onBlockCompletion() throws RestartBlockException
        {
            try {
                int recordsSize = m_records.size();
                List<Record> recordsList;
                int sleepTime = 0;
                while (recordsSize > 0) {
                    if (sleepTime > 0)
                        Thread.sleep(sleepTime);
                    // PutRecordBatchRequest can not contain more than 500 records
                    if (recordsSize > 500) {
                        recordsList = m_records.subList(0, 499);
                        m_records = new ArrayList<Record>(m_records.subList(500, recordsSize-1));
                        recordsSize = m_records.size();
                    } else {
                        recordsList = new ArrayList<Record>(m_records);
                        m_records.clear();
                        recordsSize = 0;
                    }
                    PutRecordBatchRequest batchRequest = new PutRecordBatchRequest().
                            withDeliveryStreamName(m_streamName).
                            withRecords(recordsList);
                    PutRecordBatchResult res = m_firehoseClient.putRecordBatch(batchRequest);
                    if (res.getFailedPutCount() > 0) {
                        for (PutRecordBatchResponseEntry entry : res.getRequestResponses()) {
                            if (entry.getErrorMessage() != null && !entry.getErrorMessage().contains("Slow down.")) {
                                rateLimitedLogError(m_logger, "Record failed with response: %s", entry.getErrorMessage());
                                throw new RestartBlockException(true);
                            }
                        }
                        m_records.addAll(0, recordsList);
                        recordsSize = m_records.size();
                        sleepTime = sleepTime == 0 ? 1000 : sleepTime*2;
                    } else
                        sleepTime = sleepTime == 0 ? 0 : sleepTime-10;
                }
            } catch (ResourceNotFoundException | InvalidArgumentException | ServiceUnavailableException | InterruptedException e) {
                rateLimitedLogError(m_logger, "Failed to send record batch: %s", Throwables.getStackTraceAsString(e));
                throw new RestartBlockException(true);
            }
        }
    }
}

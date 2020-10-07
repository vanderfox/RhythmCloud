package com.amazonaws.rhythmcloud.io;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.rhythmcloud.domain.TimestreamPoint;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.amazonaws.services.timestreamwrite.model.Dimension;
import com.amazonaws.services.timestreamwrite.model.Record;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsRequest;
import com.amazonaws.services.timestreamwrite.model.WriteRecordsResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class TimestreamSink extends RichSinkFunction<TimestreamPoint> implements CheckpointedFunction {
    private static final long RECORDS_FLUSH_INTERVAL_MILLISECONDS = 60L * 1000L; // One minute
    private final String region;
    private final String db;
    private final String table;
    private final Integer batchSize;
    private transient ListState<Record> checkpointedState;
    private transient AmazonTimestreamWrite writeClient;
    private List<Record> bufferedRecords;
    private long emptyListTimestamp;

    public TimestreamSink(String region, String databaseName, String tableName, int batchSize) {
        this.region = region;
        this.db = databaseName;
        this.table = tableName;
        this.batchSize = batchSize;
        this.bufferedRecords = new ArrayList<>();
        this.emptyListTimestamp = System.currentTimeMillis();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        this.writeClient = AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(this.region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    @Override
    public void invoke(TimestreamPoint value, Context context) throws Exception {
        List<Dimension> dimensions = new ArrayList<>();

        for (Map.Entry<String, String> entry : value.getDimensions().entrySet()) {
            Dimension dim = new Dimension()
                    .withName(entry.getKey())
                    .withValue(entry.getValue());
            dimensions.add(dim);
        }

        Record measure = new Record()
                .withDimensions(dimensions)
                .withMeasureName(value.getMeasureName())
                .withMeasureValueType(value.getMeasureValueType())
                .withMeasureValue(value.getMeasureValue())
                .withTimeUnit(value.getTimeUnit())
                .withTime(String.valueOf(value.getTime()));

        bufferedRecords.add(measure);

        if (shouldPublish()) {
            WriteRecordsRequest writeRecordsRequest = new WriteRecordsRequest()
                    .withDatabaseName(this.db)
                    .withTableName(this.table)
                    .withRecords(bufferedRecords);

            try {
                WriteRecordsResult writeRecordsResult = this.writeClient.writeRecords(writeRecordsRequest);
                log.debug("writeRecords Status: " + writeRecordsResult.getSdkHttpMetadata().getHttpStatusCode());
                bufferedRecords.clear();
                emptyListTimestamp = System.currentTimeMillis();
            } catch (Exception e) {
                log.error("Error: " + e);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Record element : bufferedRecords) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Record> descriptor =
                new ListStateDescriptor<>("recordList",
                        Record.class);

        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (functionInitializationContext.isRestored()) {
            for (Record element : checkpointedState.get()) {
                bufferedRecords.add(element);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    // Method to validate if record batch should be published.
    // This method would return true if the accumulated records has reached the batch size.
    // Or if records have been accumulated for last RECORDS_FLUSH_INTERVAL_MILLISECONDS time interval.
    private boolean shouldPublish() {
        if (bufferedRecords.size() == batchSize) {
            log.debug("Batch of size " + bufferedRecords.size() + " should get published");
            return true;
        } else if (System.currentTimeMillis() - emptyListTimestamp >= RECORDS_FLUSH_INTERVAL_MILLISECONDS) {
            log.debug("Records after flush interval should get published");
            return true;
        }
        return false;
    }
}

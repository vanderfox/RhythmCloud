package com.amazonaws.rhythmcloud;

import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithType;
import com.amazonaws.rhythmcloud.io.Kinesis;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class TemporalAnalyzer {
  public static void main(String[] args) throws Exception {
    try {
      log.info("Starting the rhythm analyzer");
      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      /*
      EventTime
         Event time means that the time is determined by the event's individual custom timestamp.
      IngestionTime
         Ingestion time means that the time is determined when the element enters the Flink streaming data flow.
      ProcessingTime
         Processing time for operators means that the operator uses the system clock of the machine to determine the current time of the data stream.

      https://www.bookstack.cn/read/Flink-1.10-en/4836c4072c95392f.md
      The FlinkKinesisConsumer is an exactly-once parallel streaming data source
      that subscribes to multiple AWS Kinesis streams within the same AWS service region,
      and can transparently handle resharding of streams while the job is running.
      Each subtask of the consumer is responsible for fetching data records
      from multiple Kinesis shards. The number of shards fetched by each subtask will
      change as shards are closed and created by Kinesis.
      If streaming topologies choose to use the event time notion for record timestamps,
      an approximate arrival timestamp will be used by default.
      This timestamp is attached to records by Kinesis once they were successfully
      received and stored by streams. Note that this timestamp is typically
      referred to as a Kinesis server-side timestamp, and there are no guarantees
      about the accuracy or order correctness. You can override this default with a
      custom timestamp
      */
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
      /*
      Flink’s checkpointing enabled, the Flink Kinesis Consumer will consume
      records from shards in Kinesis streams and periodically checkpoint
      each shard’s progress. In case of a job failure, Flink will restore the streaming
      program to the state of the latest complete checkpoint and re-consume
      the records from Kinesis shards.
      */
      env.enableCheckpointing(
          5_000L, CheckpointingMode.EXACTLY_ONCE); // checkpoint every 5000L- 5 seconds

      Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties();

      // Isolate metronome beat
      SingleOutputStreamOperator<DrumHitReading> metronomeHitStream =
          Kinesis.createSourceFromConfig(Constants.Stream.SYSTEMHIT, properties, env)
              .filter(
                  (FilterFunction<DrumHitReading>)
                      drumHitReading -> (drumHitReading.getDrum().equalsIgnoreCase("metronome")))
              .name("Metronome Stream");

      // Read the system hit stream without the metronome beat
      // and stamp the data with system hit
      SingleOutputStreamOperator<DrumHitReadingWithType> systemHitStream =
          Kinesis.createSourceFromConfig(Constants.Stream.SYSTEMHIT, properties, env)
              .filter(
                  (FilterFunction<DrumHitReading>)
                      drumHitReading -> (!drumHitReading.getDrum().equalsIgnoreCase("metronome")))
              .map(
                  hit ->
                      new DrumHitReadingWithType(
                          hit.getSessionId(),
                          hit.getDrum(),
                          hit.getTimestamp(),
                          hit.getVoltage(),
                          Constants.Stream.SYSTEMHIT))
              .name("System Hit Stream");

      env.execute("Temporal Analyzer");
    } catch (Exception err) {
      log.error("Temporal analyzer failed", err);
    }
  }
}

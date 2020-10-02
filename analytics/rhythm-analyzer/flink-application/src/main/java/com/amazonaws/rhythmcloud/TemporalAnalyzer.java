package com.amazonaws.rhythmcloud;

import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithType;
import com.amazonaws.rhythmcloud.io.Kinesis;
import com.amazonaws.rhythmcloud.process.ComputeBPMFunction;
import com.amazonaws.rhythmcloud.process.DrumHitReadingTSWAssigner;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class TemporalAnalyzer {
    public static void main(String[] args) throws Exception {
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            /*
             EventTime
                Event time means that the time is determined by the event's individual custom timestamp.
             IngestionTime
                Ingestion time means that the time is determined when the element enters the Flink streaming data flow.
             ProcessingTime
                Processing time for operators means that the operator uses the system clock of the machine to determine the current time of the data stream.
             */
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties();

            // Read the system hit stream
            SingleOutputStreamOperator<DrumHitReadingWithType> systemHitStream = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.SYSTEMHIT,
                    properties,
                    env)
                    .assignTimestampsAndWatermarks(new DrumHitReadingTSWAssigner())
                    .map(hit -> new DrumHitReadingWithType(
                            hit.getSessionId(),
                            hit.getDrum(),
                            hit.getTimestamp(),
                            hit.getVoltage(),
                            Constants.Stream.SYSTEMHIT));

            // Read the user hit stream
            SingleOutputStreamOperator<DrumHitReadingWithType> userHitStream = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.USERHIT,
                    properties,
                    env)
                    .assignTimestampsAndWatermarks(new DrumHitReadingTSWAssigner())
                    .map(hit -> new DrumHitReadingWithType(
                            hit.getSessionId(),
                            hit.getDrum(),
                            hit.getTimestamp(),
                            hit.getVoltage(),
                            Constants.Stream.USERHIT));

            // Compute the BPM using the metronome
            systemHitStream.union(userHitStream)
                    .keyBy(DrumHitReadingWithType::getSessionId)
                    .process(new ComputeBPMFunction());

            env.execute("Temporal Analyzer");
        } catch (Exception err) {
            log.error("Temporal analyzer failed", err);
        }
    }
}

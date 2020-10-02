package com.amazonaws.rhythmcloud;

import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithBPM;
import com.amazonaws.rhythmcloud.io.Kinesis;
import com.amazonaws.rhythmcloud.process.ComputeBPMFunction;
import com.amazonaws.rhythmcloud.process.DrumHitReadingTSWAssigner;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
            // and compute the BPM using the metronome
            DataStream<DrumHitReadingWithBPM> systemHitStreamWithBPM = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.SYSTEMHIT,
                    properties,
                    env)
                    .assignTimestampsAndWatermarks(new DrumHitReadingTSWAssigner())
                    .keyBy(DrumHitReading::getSessionId)
                    .process(new ComputeBPMFunction());

            // Read the user hit stream
            DataStream<DrumHitReading> userHitStream = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.USERHIT,
                    properties,
                    env)
                    .assignTimestampsAndWatermarks(new DrumHitReadingTSWAssigner())
                    .keyBy(DrumHitReading::getSessionId);

            env.execute("Temporal Analyzer");
        } catch (Exception err) {
            log.error("Temporal analyzer failed", err);
        }
    }
}

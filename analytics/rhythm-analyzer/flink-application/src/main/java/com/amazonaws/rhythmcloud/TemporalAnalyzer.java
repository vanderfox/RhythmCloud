package com.amazonaws.rhythmcloud;

import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import com.amazonaws.rhythmcloud.io.Kinesis;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class TemporalAnalyzer {
    public static void main(String[] args) throws Exception {
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Map<String, Properties> properties = KinesisAnalyticsRuntime.getApplicationProperties();

            // Read the system hit stream
            DataStream<DrumHitReading> systemHitStream = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.SYSTEMHIT,
                    properties,
                    env);

            // Read the user hit stream
            DataStream<DrumHitReading> userHitStream = Kinesis.createSourceFromApplicationProperties(
                    Constants.Stream.USERHIT,
                    properties,
                    env);

            env.execute("Temporal Analyzer");
        } catch (Exception err) {
            log.error("Temporal analyzer failed", err);
        }
    }
}

package com.amazonaws.rhythmcloud.io;

import com.amazonaws.rhythmcloud.Constants;
import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Map;
import java.util.Properties;

import static com.amazonaws.rhythmcloud.Constants.DEFAULT_REGION_NAME;

@Slf4j
public class Kinesis {
    public static DataStream<DrumHitReading> createSourceFromApplicationProperties(
            Constants.Stream stream,
            Map<String, Properties> applicationProperties,
            StreamExecutionEnvironment env) {
        Properties sourceProperties = applicationProperties.get(
                Constants.propertyGroupNames.get(stream));

        // Throw exception if you cannot find the source properties
        if (sourceProperties == null) {
            String errorMessage = String.format("Unable to load %s property group from the Kinesis Analytics Runtime.",
                    Constants.propertyGroupNames.get(stream));
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        // Make sure all the mandatory properties are set: REGION, STREAMPOSITION et al.
        if (sourceProperties.getProperty(AWSConfigConstants.AWS_REGION) == null) {
            //set the region the Kinesis stream is located in
            sourceProperties.put(AWSConfigConstants.AWS_REGION,
                    Regions.getCurrentRegion() == null ? DEFAULT_REGION_NAME :
                            Regions.getCurrentRegion().getName());
        }
        if (sourceProperties.getProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION) == null) {
            //stream initialization position
            sourceProperties.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                    Constants.STREAM_LATEST_POSITION);
        }
        if (sourceProperties.getProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS) == null) {
            //poll interval to poll for new events from Kinesis stream
            //default is 1 second
            sourceProperties.put(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
                    Constants.STREAM_POLL_INTERVAL);
        }
        //obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
        sourceProperties.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

        log.info("Source Properties {}", sourceProperties.toString());

        return env.addSource(new FlinkKinesisConsumer<>(
                sourceProperties.getProperty("input.stream.name",
                        Constants.streamNames.get(stream)),
                new DrumHitReadingDeSerializer(),
                sourceProperties));
    }
}

//package com.amazonaws.rhythmcloud.io;
//
//import com.amazonaws.rhythmcloud.Constants;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
//
//import java.util.Map;
//import java.util.Properties;
//
//@Slf4j
//public class KinesisSink<T, TS> {
//    public static FlinkKinesisProducer<T> createSink(
//            Map<String, Properties> applicationProperties,
//            StreamExecutionEnvironment env) {
//        Properties sinkProperties = applicationProperties.get(Constants.SINK_PROPERTY_GROUP);
//
//        if (sinkProperties == null) {
//            log.error("Unable to load {} properties from the Kinesis Analytics Runtime.", Constants.SINK_PROPERTY_GROUP);
//            throw new RuntimeException(String.format(
//                    "Unable to load %s properties from the Kinesis Analytics Runtime.",
//                    Constants.SINK_PROPERTY_GROUP));
//        }
//
//        if (sinkProperties.getProperty(AWSConfigConstants.AWS_REGION) == null) {
//            sinkProperties.put(AWSConfigConstants.AWS_REGION,
//                    Regions.getCurrentRegion() == null ? Constants.DEFAULT_REGION_NAME :
//                            Regions.getCurrentRegion().getName());
//        }
//        sinkProperties.put(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
//        log.info(sinkProperties.toString());
//
//        FlinkKinesisProducer<T> flinkKinesisProducer = new FlinkKinesisProducer<>(
//                new DeviceAggregateEventSerializationSchema(),
//                sinkProperties);
//
//        flinkKinesisProducer.setFailOnError(true);
//        flinkKinesisProducer.setDefaultPartition("0");  // Partition Key
//        flinkKinesisProducer.setQueueLimit(1);
//        flinkKinesisProducer.setDefaultStream(
//                sinkProperties.getProperty("output.stream.name", Constants.OUTPUT_STREAM_NAME));
//
//        return flinkKinesisProducer;
//    }
//}

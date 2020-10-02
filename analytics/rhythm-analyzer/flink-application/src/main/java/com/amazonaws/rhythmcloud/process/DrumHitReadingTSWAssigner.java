package com.amazonaws.rhythmcloud.process;

import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

@SuppressWarnings("serial")
public class DrumHitReadingTSWAssigner extends AscendingTimestampExtractor<DrumHitReading> {
    @Override
    public long extractAscendingTimestamp(DrumHitReading drumHitReading) {
        return drumHitReading.getTimestamp().toEpochMilli();
    }
}

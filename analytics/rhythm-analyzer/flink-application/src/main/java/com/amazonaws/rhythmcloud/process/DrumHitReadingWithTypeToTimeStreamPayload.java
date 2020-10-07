package com.amazonaws.rhythmcloud.process;

import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithType;
import com.amazonaws.rhythmcloud.domain.TimestreamPoint;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DrumHitReadingWithTypeToTimeStreamPayload extends RichMapFunction<DrumHitReadingWithType, TimestreamPoint> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public TimestreamPoint map(DrumHitReadingWithType hit) {
        TimestreamPoint dataPoint = new TimestreamPoint();
        Map<String, String> dimensions = new HashMap<>(5);

        dataPoint.setTime(hit.getTimestamp());
        dataPoint.setTimeUnit(TimeUnit.MILLISECONDS.toString());
        dataPoint.setMeasureValue(String.valueOf(hit.getVoltage()));
        dataPoint.setMeasureValueType(MeasureValueType.DOUBLE);
        dimensions.put("drum", hit.getDrum());
        dimensions.put("session",
                String.valueOf(hit.getSessionId()));
        dimensions.put("type",
                String.valueOf(hit.getType().getStreamCode()));
        dataPoint.setDimensions(dimensions);

        return dataPoint;
    }
}

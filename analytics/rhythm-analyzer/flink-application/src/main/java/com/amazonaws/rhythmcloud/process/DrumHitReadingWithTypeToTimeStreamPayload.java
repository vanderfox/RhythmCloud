package com.amazonaws.rhythmcloud.process;

import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithType;
import com.amazonaws.rhythmcloud.domain.TimeStreamPoint;
import com.amazonaws.services.timestreamwrite.model.DimensionValueType;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DrumHitReadingWithTypeToTimeStreamPayload extends RichMapFunction<DrumHitReadingWithType, TimeStreamPoint> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public TimeStreamPoint map(DrumHitReadingWithType hit) {
        TimeStreamPoint dataPoint = new TimeStreamPoint();
        Map<String, Tuple2<DimensionValueType, String>> dimensions = new HashMap<>(5);

        dataPoint.setTime(hit.getTimestamp());
        dataPoint.setTimeUnit(TimeUnit.MILLISECONDS.toString());
        dataPoint.setMeasureValue(String.valueOf(hit.getVoltage()));
        dataPoint.setMeasureValueType(MeasureValueType.DOUBLE);
        dimensions.put("drum", new Tuple2<>(DimensionValueType.VARCHAR, hit.getDrum()));
        dimensions.put("session", new Tuple2<>(
                DimensionValueType.fromValue("BIGINT"),
                String.valueOf(hit.getSessionId())));
        dimensions.put("type", new Tuple2<>(
                DimensionValueType.fromValue("INTEGER"),
                String.valueOf(hit.getType().getStreamCode())));
        dataPoint.setDimensions(dimensions);

        return dataPoint;
    }
}

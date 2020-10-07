package com.amazonaws.rhythmcloud.domain;

import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@AllArgsConstructor
@Data
@Getter
@Setter
public class TimestreamPoint {
    private String measureName;
    private MeasureValueType measureValueType;
    private String measureValue;
    private long time;
    private String timeUnit;
    private Map<String, String> dimensions;

    public TimestreamPoint() {
        this.dimensions = new HashMap<>();
    }

}

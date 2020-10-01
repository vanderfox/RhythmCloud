package com.amazonaws.rhythmcloud.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DrumHitReading {
    @JsonProperty("sessionId")
    private Long sessionId;
    @JsonProperty("drum")
    private String drum;
    @JsonProperty("timestamp")
    private Instant timestamp;
    @JsonProperty("voltage")
    private Double voltage;
}

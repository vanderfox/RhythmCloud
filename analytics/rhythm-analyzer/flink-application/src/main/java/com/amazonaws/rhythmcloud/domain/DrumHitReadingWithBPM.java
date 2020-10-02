package com.amazonaws.rhythmcloud.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DrumHitReadingWithBPM implements Comparable<DrumHitReadingWithBPM> {
    @JsonProperty("sessionId")
    private Long sessionId;
    @JsonProperty("drum")
    private String drum;
    @JsonProperty("timestamp")
    private Instant timestamp;
    @JsonProperty("voltage")
    private Double voltage;
    @JsonProperty("bpm_ms")
    private Long bpmInMilliSeconds;

    @Override
    public int compareTo(DrumHitReadingWithBPM o) {
        Duration d = Duration.between(this.getTimestamp(), o.getTimestamp());
        if (d.toMillis() == 0)
            return 0;
        else if (d.toMillis() > 0)
            return 1;
        else
            return -1;
    }
}

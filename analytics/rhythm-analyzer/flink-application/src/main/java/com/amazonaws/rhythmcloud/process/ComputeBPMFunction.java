package com.amazonaws.rhythmcloud.process;

import com.amazonaws.rhythmcloud.Constants;
import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithBPM;
import com.amazonaws.rhythmcloud.domain.DrumHitReadingWithType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

public class ComputeBPMFunction extends KeyedProcessFunction<Long, DrumHitReadingWithType, DrumHitReadingWithBPM> {
    ValueState<Long> bpmInMilliSecondsState = null;
    ValueState<Instant> lastMetronomeInstantState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> bpmStateDescriptor = new ValueStateDescriptor<>(
                "BPM state", Long.class);
        ValueStateDescriptor<Instant> metronomeInstantDescriptor = new ValueStateDescriptor<>(
                "Metronome state", Instant.class);
        bpmInMilliSecondsState = getRuntimeContext().getState(bpmStateDescriptor);
        lastMetronomeInstantState = getRuntimeContext().getState(metronomeInstantDescriptor);
    }

    @Override
    public void processElement(DrumHitReadingWithType drumHitReading, Context context, Collector<DrumHitReadingWithBPM> collector) throws Exception {
        // Filter out the metronome which appears only in the system hit stream
        // Rather use it to compute beats per minute
        if (drumHitReading.getType().equals(Constants.Stream.SYSTEMHIT) &&
                drumHitReading.getDrum().equalsIgnoreCase("metronome")) {
            Instant lastMetronomeInstant = lastMetronomeInstantState.value();
            // Very first hit for this session
            if (lastMetronomeInstant == null) {
                bpmInMilliSecondsState.update(0L);
            } else {
                Duration duration = Duration.between(lastMetronomeInstant, drumHitReading.getTimestamp());
                // Optimization to help checkpointing:
                // Update only if BPM is different
                Long bpmInMilliSeconds = bpmInMilliSecondsState.value();
                if (duration.toMillis() != bpmInMilliSeconds) {
                    bpmInMilliSecondsState.update(duration.toMillis());
                }
            }
            // Update the last hit instant for this session
            lastMetronomeInstantState.update(drumHitReading.getTimestamp());
        } else {
            // Only collect the data when BPM is meaningful
            Long bpmInMilliSeconds = bpmInMilliSecondsState.value();
            if (bpmInMilliSeconds != null && bpmInMilliSeconds > 0L) {
                DrumHitReadingWithBPM drumHitReadingWithBPM = new DrumHitReadingWithBPM(
                        drumHitReading.getSessionId(),
                        drumHitReading.getDrum(),
                        drumHitReading.getTimestamp(),
                        drumHitReading.getVoltage(),
                        drumHitReading.getType(),
                        bpmInMilliSecondsState.value());
                collector.collect(drumHitReadingWithBPM);
            }
        }
    }
}

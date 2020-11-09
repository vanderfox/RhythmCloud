package com.amazonaws.rhythmcloud.process;

import com.amazonaws.rhythmcloud.domain.DrumBeat;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class MetronomeFilterFn implements SerializableFunction<KV<String, DrumBeat>, Boolean> {
  @Override
  public Boolean apply(KV<String, DrumBeat> input) {
    log.info("Filtering {}", input.getValue().toString());
    return !(input.getValue().getDrum().equalsIgnoreCase("metronome"));
  }
}

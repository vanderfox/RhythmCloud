package com.amazonaws.rhythmcloud.domain;


import com.google.gson.*;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class DrumHitReadingDeserializer implements JsonDeserializer<DrumHitReading> {
    @Override
    public DrumHitReading deserialize(JsonElement json,
                                      Type typeOfT,
                                      JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        String nanoSeconds = jsonObject.get("timestamp").getAsString();
        String[] nanoSecondsAsParts = nanoSeconds.split("\\.");
        Instant timestamp = Instant.EPOCH
                .plus(
                        Long.parseLong(nanoSecondsAsParts[0]),
                        ChronoUnit.MILLIS)
                .plus(Duration.of(Long.parseLong(nanoSecondsAsParts[1]),
                        ChronoUnit.MILLIS));

        return new DrumHitReading(
                jsonObject.get("sessionId").getAsLong(),
                jsonObject.get("drum").getAsString(),
                timestamp,
                jsonObject.get("voltage").getAsDouble()
        );
    }
}

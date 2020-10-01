package com.amazonaws.rhythmcloud.io;

import com.amazonaws.rhythmcloud.domain.DrumHitReading;
import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
public class DrumHitReadingDeSerializer extends AbstractDeserializationSchema<DrumHitReading> {
    private static final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY) // IDENTITY: This policy with Gson will ensure that the field name is unchanged.
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
            .create();

    public DrumHitReading deserialize(byte[] bytes) throws IOException {
        try {
            JsonReader jsonReader = new JsonReader(
                    new InputStreamReader(new ByteArrayInputStream(bytes)));
            JsonElement jsonElement = Streams.parse(jsonReader);
            DrumHitReading reading = gson.fromJson(jsonElement, DrumHitReading.class);
            log.info("Successfully translated: {}", reading);
            return reading;
        } catch (Exception e) {
            log.error("cannot parse event '{}'", new String(bytes, StandardCharsets.UTF_8), e);
            return null;
        }
    }

    public boolean isEndOfStream(DrumHitReading event) {
        return false;
    }

    @Override
    public TypeInformation<DrumHitReading> getProducedType() {
        return TypeExtractor.getForClass(DrumHitReading.class);
    }
}

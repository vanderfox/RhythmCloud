package com.amazonaws.rhythmcloud;

import java.util.EnumMap;

public final class Constants {
    public static final String DEFAULT_REGION_NAME = "us-east-1";

    public static final String STREAM_LATEST_POSITION = "LATEST";

    public static final String STREAM_POLL_INTERVAL = "1000";

    public enum Stream {
        SYSTEMHIT(1),
        USERHIT(2),
        TEMPORALANALYSIS(3);

        private final int streamCode;

        Stream(int streamCode) {
            this.streamCode = streamCode;
        }

        public int getStreamCode() {
            return this.streamCode;
        }
    }

    public static final EnumMap<Stream, String> propertyGroupNames =
            new EnumMap<>(Stream.class);

    public static final EnumMap<Stream, String> streamNames =
            new EnumMap<>(Stream.class);

    public Constants() {
        propertyGroupNames.put(Stream.SYSTEMHIT, "SYSTEMHIT");
        propertyGroupNames.put(Stream.USERHIT, "USERHIT");
        propertyGroupNames.put(Stream.TEMPORALANALYSIS, "TEMPORALANALYSIS");

        streamNames.put(Stream.SYSTEMHIT, "rhythm-cloud-system-hit-stream");
        streamNames.put(Stream.USERHIT, "rhythm-cloud-user-hit-stream");
        streamNames.put(Stream.TEMPORALANALYSIS, "rhythm-cloud-analysis-output-stream");
    }
}

package com.dtc.java.analytic.V1.snmp;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Metric Schema ，支持序列化和反序列化
 *
 */
public class SourceEventSchema implements DeserializationSchema<SourceEvent>, SerializationSchema<SourceEvent> {

    private static final Gson gson = new Gson();

    @Override
    public SourceEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), SourceEvent.class);
    }

    @Override
    public boolean isEndOfStream(SourceEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(SourceEvent metricEvent) {
        return gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<SourceEvent> getProducedType() {
        return TypeInformation.of(SourceEvent.class);
    }
}

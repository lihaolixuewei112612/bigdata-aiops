package com.dtc.java.analytic.V2.common.schemas;

import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
public class SourceEventSchema implements DeserializationSchema<SourceEvent>, SerializationSchema<SourceEvent> {

    private static final Gson gson = new Gson();

    @Override
    public SourceEvent deserialize(byte[] bytes) {
        return gson.fromJson(new String(bytes), SourceEvent.class);
    }

    @Override
    public boolean isEndOfStream(SourceEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(SourceEvent sourceEvent) {
        return gson.toJson(sourceEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<SourceEvent> getProducedType() {
        return TypeInformation.of(SourceEvent.class);
    }
}

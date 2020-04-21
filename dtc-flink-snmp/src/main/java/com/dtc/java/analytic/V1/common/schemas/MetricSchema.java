package com.dtc.java.analytic.V1.common.schemas;

import com.dtc.java.analytic.V1.common.model.MetricEvent;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created on 2019-10-17
 *
 * @author :hao.li
 */
public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private static final Gson gson = new Gson();

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        return gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}

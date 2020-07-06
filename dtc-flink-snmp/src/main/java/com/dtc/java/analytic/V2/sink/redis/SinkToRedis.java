package com.dtc.java.analytic.V2.sink.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import static com.dtc.java.analytic.V2.worker.untils.MainUntils.writeEventToHbase;

/**
 * @Author : lihao
 * Created on : 2020-06-19
 * @Description : TODO描述类作用
 */
public class SinkToRedis extends RichSinkFunction<Tuple3<String, String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(SinkToRedis.class);

    private Jedis jedis = null;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        jedis = RedisUtil.getResource(parameterTool);
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        if ("102_101_101_101_101".equals(value.f0)||"102_103_101_101_101".equals(value.f0)||"103_102_101_101_101".equals(value)) {
            jedis.sadd(value.f0 + "-" + 1, value.f1);
        } else if ("102_101_103_107_108".equals(value.f0)||"102_103_103_105_105".equals(value.f0)||"103_102_103_107_107_1".equals(value)) {
            jedis.sadd(value.f0 + "-" + 2, value.f1);
        }
        jedis.expire(value.f0, 60000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != jedis) {
            jedis.close();
        }
    }
}

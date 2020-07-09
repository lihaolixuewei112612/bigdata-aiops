package com.dtc.java.analytic.V2.sink.redis;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author : lihao
 * Created on : 2020-06-19
 * @Description : TODO描述类作用
 */
public class RedisUtil {
    private static volatile JedisPool jedisPool = null;
    public static Jedis getResource(ParameterTool parameterTool) {
        String ip = parameterTool.get(PropertiesConstants.REDIS_IP).trim();
        String port = parameterTool.get(PropertiesConstants.REDIS_PORT).trim();
        String passwd = parameterTool.get(PropertiesConstants.REDIS_PW).trim();
        if (null == jedisPool) {
            synchronized (RedisUtil.class) {
                if (null == jedisPool) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(1000);
                    config.setMaxIdle(100);
                    config.setMaxWaitMillis(5000);
                    config.setTestOnBorrow(true);
                    jedisPool = new JedisPool(config, ip, Integer.parseInt(port),5000,passwd);
                }
            }
        }
        return jedisPool.getResource();
    }

}

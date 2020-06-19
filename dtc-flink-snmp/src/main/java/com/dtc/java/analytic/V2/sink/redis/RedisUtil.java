package com.dtc.java.analytic.V2.sink.redis;

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

    public static Jedis getResource() {
        if (null == jedisPool) {
            synchronized (RedisUtil.class) {
                if (null == jedisPool) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(4);
                    config.setMaxIdle(1);
                    config.setMaxWaitMillis(100);
                    config.setTestOnBorrow(true);
                    jedisPool = new JedisPool(config, "10.3.7.233", 6379,2000,"111111");
                }
            }
        }
        return jedisPool.getResource();
    }

}

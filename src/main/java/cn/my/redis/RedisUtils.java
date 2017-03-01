package cn.my.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * 实现一个redis的工具类
 * 这里面首先要有一个获取连接的方法
 * 还要有一个返回连接的方法
 * @author Administrator
 *
 */
public class RedisUtils {
	
	public static JedisPool jedisPool= null;
	public static synchronized Jedis getJedis()
	{
		if(jedisPool==null){
			GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
			//设置最大空闲连接数
			poolConfig.setMaxIdle(10);
			//连接池中最大连接数
			poolConfig.setMaxTotal(100);
			//在获取链接的时候设置的超时时间
			poolConfig.setMaxWaitMillis(1000);
			//设置连接池在创建连接的时候，需要对新创建的连接进行测试，保证连接池中的连接都是可用的
			poolConfig.setTestOnBorrow(true);
			jedisPool = new JedisPool(poolConfig, "spark4",6379);
		}
		return jedisPool.getResource();
	}
	
	/**
	 * 把连接放到连接池中
	 * @param jedis
	 */
	public static void returnJedis(Jedis jedis){
		jedisPool.returnResourceObject(jedis);
	}
}

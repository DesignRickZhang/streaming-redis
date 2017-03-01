package cn.my.redis;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

public class RedisTest {
	
	String host = "spark4";
	int port = 6379;
	Jedis jedis = new Jedis(host, port);
	/**
	 * 单机测试方式
	 * 
	 * 使用java获取redis的连接
	 * 测试环境使用这种方式
	 * @throws Exception
	 */
	@Test
	public void test() throws Exception {
		jedis.set("crxy1", "abcde");
		
		String value = jedis.get("crxy1");
		System.out.println(value);
	}
	
	/**
	 * 单机连接池方式
	 * 使用redis的连接池
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		//设置最大空闲连接数
		poolConfig.setMaxIdle(10);
		//连接池中最大连接数
		poolConfig.setMaxTotal(100);
		//在获取链接的时候设置的超时时间
		poolConfig.setMaxWaitMillis(1000);
		//设置连接池在创建连接的时候，需要对新创建的连接进行测试，保证连接池中的连接都是可用的
		poolConfig.setTestOnBorrow(true);
		
		JedisPool jedisPool = new JedisPool(poolConfig, host,port);
		Jedis jedis = jedisPool.getResource();
		String value = jedis.get("tom");
		System.out.println(value);
		
		jedisPool.returnResourceObject(jedis);
	}
	
	
	/**
	 * 限制用户的访问频率
	 * 一分钟之内最多只能访问10次
	 * @throws Exception
	 */
	@Test
	public void test3() throws Exception {
		String ip = host;
		for(int i=0;i<20;i++){
			boolean flag = testLogin(ip);
			System.out.println(i+"------"+flag);
		}
		
		
	}
	
	/**
	 * 验证用户是否可以登录
	 * @param ip
	 * @return
	 */
	public boolean testLogin(String ip){
		String value = jedis.get(ip);
		if(value==null){
			jedis.set(ip, 1+"");
			//记得要给这个键设置生存时间
			jedis.expire(ip, 60);
		}else {
			int parseInt = Integer.parseInt(value);
			if(parseInt>10){
				System.out.println("访问过快！");
				return false;
			}else{
				jedis.incr(ip);
			}
			
		}
		return true;
	}
	
	
	/**
	 * 自己实现一个incr命令
	 * @throws Exception
	 */
	@Test
	public void testIncr() throws Exception {
		jedis.watch("x");
		String value = jedis.get("x");
		//监控x
		int parseInt = Integer.parseInt(value);
		parseInt++;
		System.out.println("暂停一会！");
		Thread.sleep(3000);
		Transaction multi = jedis.multi();
		multi.set("x", parseInt+"");
		List<Object> exec = multi.exec();
		//事务执行完成之后，键的监控状态就取消了
		if(exec==null){
			System.out.println("x的值被修改了，事务中的命令没有执行");
			testIncr();
		}else{
			System.out.println("正常执行了。");
		}	
		
	}
	
	
	
	
	/**
	 * 向redis数据库添加1万条数据
	 * 不使用管道
	 * 消耗时间：6221
	 * @throws Exception
	 */
	@Test
	public void test4() throws Exception {
		long start_time = System.currentTimeMillis();
		for(int i=0;i<10000;i++){
			jedis.set("m"+i, "m"+i);
		}
		long end_time = System.currentTimeMillis();
		System.out.println(end_time-start_time);
	}
	/**
	 * 向redis数据库添加1万条数据
	 * 使用管道
	 * 消耗时间：191
	 * @throws Exception
	 */
	@Test
	public void test5() throws Exception {
		Pipeline pipelined = jedis.pipelined();
		long start_time = System.currentTimeMillis();
		for(int i=0;i<10000;i++){
			pipelined.set("m"+i, "m"+i);
		}
		pipelined.sync();
		long end_time = System.currentTimeMillis();
		System.out.println(end_time-start_time);
		
	}
	
	
	/**
	 * 使用sentinel来操作redis集群
	 * @throws Exception
	 */
	@Test
	public void test6() throws Exception {
		
		String masterName = "mymaster";
		Set<String> sentinels = new HashSet<String>();
		sentinels.add("192.168.1.172:26379");
		sentinels.add("192.168.1.173:26379");
		
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		//TODO---
		JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(masterName , sentinels , poolConfig );
		HostAndPort currentHostMaster = jedisSentinelPool.getCurrentHostMaster();
		System.out.println(currentHostMaster.getHost()+"--"+currentHostMaster.getPort());
		Jedis jedis = jedisSentinelPool.getResource();
		jedis.set("a", "1");
		jedisSentinelPool.returnResourceObject(jedis);
		
		
		
	}
	
	
	/**
	 * 操作redis集群
	 * @throws Exception
	 */
	@Test
	public void test7() throws Exception {
		Set<HostAndPort> nodes = new HashSet<HostAndPort>();
		nodes.add(new HostAndPort("192.168.1.170", 7000));
		nodes.add(new HostAndPort("192.168.1.170", 7001));
		nodes.add(new HostAndPort("192.168.1.170", 7002));
		nodes.add(new HostAndPort("192.168.1.170", 7003));
		nodes.add(new HostAndPort("192.168.1.170", 7004));
		nodes.add(new HostAndPort("192.168.1.170", 7005));
		
		
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		//TODO--
		JedisCluster jedisCluster = new JedisCluster(nodes , poolConfig );
		jedisCluster.set("a", "10");
		String value = jedisCluster.get("a");
		System.out.println(value);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	

}

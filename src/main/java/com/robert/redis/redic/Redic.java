package com.robert.redis.redic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster.Reset;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.util.Pool;
import redis.clients.util.Slowlog;

import com.robert.redis.redic.excep.NotSupportedException;
import com.robert.redis.redic.strategy.HashShardingStrategy;
import com.robert.redis.redic.strategy.ShardingStrategy;

public class Redic extends Jedis {
	private List<RedicNode> redicNodes = new ArrayList<RedicNode>();

	private ShardingStrategy shardingStategy = new HashShardingStrategy();

	private boolean readWriteSeparate = false;
	
	private List<String> nodeConnStrs;

	public Redic() {
	}

	public Redic(List<String> nodeConnStrs) {
		this.nodeConnStrs = nodeConnStrs;
		init();
	}
	
	public void init() {
		for (String nodeConnStr : nodeConnStrs)
			this.addNode(nodeConnStr);
	}

	public Redic addNode(String nodeConnStr) {
		String[] nodes = nodeConnStr.split(RedicNode.NODE_SEPARATOR);

		return addNode(nodes[0], Arrays.asList(Arrays.copyOf(nodes, 1)));
	}

	public Redic addNode(String jedisConnStr, List<String> slaveConnStrs) {
		redicNodes.add(new RedicNode(jedisConnStr, slaveConnStrs));

		return this;
	}

	protected <T> Jedis getWrite(T key) {
		int nodeIndex = shardingStategy.key2node(key, redicNodes.size());
		RedicNode redicNode = redicNodes.get(nodeIndex);

		return redicNode.getMaster().getResource();
	}

	protected <T> Jedis getRead(T key) {
		int nodeIndex = shardingStategy.key2node(key, redicNodes.size());
		RedicNode redicNode = redicNodes.get(nodeIndex);

		if (!readWriteSeparate)
			return redicNode.getMaster().getResource();

		return redicNode.getRoundRobinSlaveRedicNode().getResource();
	}

	@Override
	public String set(String key, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.set(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public String set(String key, String value, String nxxx, String expx,
			long time) {
		Jedis jedis = getWrite(key);
		String ret = jedis.set(key, value, nxxx, expx, time);
		jedis.close();

		return ret;
	}

	@Override
	public String get(String key) {
		Jedis jedis = getRead(key);
		String ret = jedis.get(key);
		jedis.close();

		return ret;
	}

	@Override
	public Boolean exists(String key) {
		Jedis jedis = getRead(key);
		Boolean ret = jedis.exists(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long del(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Long del(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.del(key);
		jedis.close();

		return ret;
	}

	@Override
	public String type(String key) {
		Jedis jedis = getRead(key);
		String ret = jedis.type(key);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> keys(String pattern) {
		throw new NotSupportedException();
	}

	@Override
	public String randomKey() {
		throw new NotSupportedException();
	}

	@Override
	public String rename(String oldkey, String newkey) {
		Jedis source = getWrite(oldkey);
		Jedis dest = getWrite(newkey);
		String value = source.get(oldkey);
		String ret = dest.set(newkey, value);
		source.del(oldkey);

		source.close();
		dest.close();

		return ret;
	}

	@Override
	public Long renamenx(String oldkey, String newkey) {
		throw new NotSupportedException();
	}

	@Override
	public Long expire(String key, int seconds) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.expire(key, seconds);
		jedis.close();

		return ret;
	}

	@Override
	public Long expireAt(String key, long unixTime) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.expireAt(key, unixTime);
		jedis.close();

		return ret;
	}

	@Override
	public Long ttl(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.ttl(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long move(String key, int dbIndex) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.move(key, dbIndex);
		jedis.close();

		return ret;
	}

	@Override
	public String getSet(String key, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.getSet(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> mget(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Long setnx(String key, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.setnx(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public String setex(String key, int seconds, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.setex(key, seconds, value);
		jedis.close();

		return ret;
	}

	@Override
	public String mset(String... keysvalues) {
		throw new NotSupportedException();
	}

	@Override
	public Long msetnx(String... keysvalues) {
		throw new NotSupportedException();
	}

	@Override
	public Long decrBy(String key, long integer) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.decrBy(key, integer);
		jedis.close();

		return ret;
	}

	@Override
	public Long decr(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.decr(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long incrBy(String key, long integer) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.incrBy(key, integer);
		jedis.close();

		return ret;
	}

	@Override
	public Double incrByFloat(String key, double value) {
		Jedis jedis = getWrite(key);
		Double ret = jedis.incrByFloat(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public Long incr(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.incr(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long append(String key, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.append(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public String substr(String key, int start, int end) {
		Jedis jedis = getRead(key);
		String ret = jedis.substr(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long hset(String key, String field, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.hset(key, field, value);
		jedis.close();

		return ret;
	}

	@Override
	public String hget(String key, String field) {
		Jedis jedis = getRead(key);
		String ret = jedis.hget(key, field);
		jedis.close();

		return ret;
	}

	@Override
	public Long hsetnx(String key, String field, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.hsetnx(key, field, value);
		jedis.close();

		return ret;
	}

	@Override
	public String hmset(String key, Map<String, String> hash) {
		Jedis jedis = getWrite(key);
		String ret = jedis.hmset(key, hash);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.hmget(key, fields);
		jedis.close();

		return ret;
	}

	@Override
	public Long hincrBy(String key, String field, long value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.hincrBy(key, field, value);
		jedis.close();

		return ret;
	}

	@Override
	public Double hincrByFloat(String key, String field, double value) {
		Jedis jedis = getWrite(key);
		Double ret = jedis.hincrByFloat(key, field, value);
		jedis.close();

		return ret;
	}

	@Override
	public Boolean hexists(String key, String field) {
		Jedis jedis = getRead(key);
		Boolean ret = jedis.hexists(key, field);
		jedis.close();

		return ret;
	}

	@Override
	public Long hdel(String key, String... fields) {
		throw new NotSupportedException();
	}

	@Override
	public Long hlen(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.hlen(key);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> hkeys(String key) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.hkeys(key);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> hvals(String key) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.hvals(key);
		jedis.close();

		return ret;
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		Jedis jedis = getRead(key);
		Map<String, String> ret = jedis.hgetAll(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long rpush(String key, String... strings) {
		throw new NotSupportedException();
	}

	@Override
	public Long lpush(String key, String... strings) {
		throw new NotSupportedException();
	}

	@Override
	public Long llen(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.llen(key);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.lrange(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public String ltrim(String key, long start, long end) {
		Jedis jedis = getRead(key);
		String ret = jedis.ltrim(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public String lindex(String key, long index) {
		Jedis jedis = getRead(key);
		String ret = jedis.lindex(key, index);
		jedis.close();

		return ret;
	}

	@Override
	public String lset(String key, long index, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.lset(key, index, value);
		jedis.close();

		return ret;
	}

	@Override
	public Long lrem(String key, long count, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.lrem(key, count, value);
		jedis.close();

		return ret;
	}

	@Override
	public String lpop(String key) {
		Jedis jedis = getWrite(key);
		String ret = jedis.lpop(key);
		jedis.close();

		return ret;
	}

	@Override
	public String rpop(String key) {
		Jedis jedis = getWrite(key);
		String ret = jedis.rpop(key);
		jedis.close();

		return ret;
	}

	@Override
	public String rpoplpush(String srckey, String dstkey) {
		throw new NotSupportedException();
	}

	@Override
	public Long sadd(String key, String... members) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.sadd(key, members);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> smembers(String key) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.smembers(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long srem(String key, String... members) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.srem(key, members);
		jedis.close();

		return ret;
	}

	@Override
	public String spop(String key) {
		Jedis jedis = getWrite(key);
		String ret = jedis.spop(key);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> spop(String key, long count) {
		Jedis jedis = getWrite(key);
		Set<String> ret = jedis.spop(key, count);
		jedis.close();

		return ret;
	}

	@Override
	public Long smove(String srckey, String dstkey, String member) {
		throw new NotSupportedException();
	}

	@Override
	public Long scard(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.scard(key);
		jedis.close();

		return ret;
	}

	@Override
	public Boolean sismember(String key, String member) {
		Jedis jedis = getWrite(key);
		Boolean ret = jedis.sismember(key, member);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> sinter(String... keys) {
		throw new NotSupportedException();

	}

	@Override
	public Long sinterstore(String dstkey, String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Set<String> sunion(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Long sunionstore(String dstkey, String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Set<String> sdiff(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Long sdiffstore(String dstkey, String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public String srandmember(String key) {
		Jedis jedis = getRead(key);
		String ret = jedis.srandmember(key);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> srandmember(String key, int count) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.srandmember(key, count);
		jedis.close();

		return ret;
	}

	@Override
	public Long zadd(String key, double score, String member) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zadd(key, score, member);
		jedis.close();

		return ret;
	}

	@Override
	public Long zadd(String key, Map<String, Double> scoreMembers) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zadd(key, scoreMembers);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrange(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long zrem(String key, String... members) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zrem(key, members);
		jedis.close();

		return ret;
	}

	@Override
	public Double zincrby(String key, double score, String member) {
		Jedis jedis = getWrite(key);
		Double ret = jedis.zincrby(key, score, member);
		jedis.close();

		return ret;
	}

	@Override
	public Long zrank(String key, String member) {
		Jedis jedis = getRead(key);
		Long ret = jedis.zrank(key, member);
		jedis.close();

		return ret;
	}

	@Override
	public Long zrevrank(String key, String member) {
		Jedis jedis = getRead(key);
		Long ret = jedis.zrevrank(key, member);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrange(String key, long start, long end) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrange(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrangeWithScores(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrevrangeWithScores(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long zcard(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.zcard(key);
		jedis.close();

		return ret;
	}

	@Override
	public Double zscore(String key, String member) {
		Jedis jedis = getRead(key);
		Double ret = jedis.zscore(key, member);
		jedis.close();

		return ret;
	}

	@Override
	public String watch(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> sort(String key) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.sort(key);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> sort(String key, SortingParams sortingParameters) {
		Jedis jedis = getRead(key);
		List<String> ret = jedis.sort(key, sortingParameters);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> blpop(int timeout, String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> blpop(String... args) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> brpop(String... args) {
		throw new NotSupportedException();
	}

	@Override
	@Deprecated
	public List<String> blpop(String arg) {
		Jedis jedis = getWrite(arg);
		List<String> ret = jedis.blpop(arg);
		jedis.close();

		return ret;

	}

	@Override
	@Deprecated
	public List<String> brpop(String arg) {
		Jedis jedis = getWrite(arg);
		List<String> ret = jedis.brpop(arg);
		jedis.close();

		return ret;
	}

	@Override
	public Long sort(String key, SortingParams sortingParameters, String dstkey) {
		throw new NotSupportedException();
	}

	@Override
	public Long sort(String key, String dstkey) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> brpop(int timeout, String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public Long zcount(String key, double min, double max) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zcount(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Long zcount(String key, String min, String max) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zcount(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByScore(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByScore(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByScore(key, min, max, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByScore(key, min, max, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrangeByScoreWithScores(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrangeByScoreWithScores(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min,
			double max, int offset, int count) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrangeByScoreWithScores(key, min, max, offset,
				count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min,
			String max, int offset, int count) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrangeByScoreWithScores(key, min, max, offset,
				count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByScore(key, max, min);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByScore(key, max, min);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByScore(key, max, min, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrevrangeByScoreWithScores(key, max, min);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max,
			double min, int offset, int count) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrevrangeByScoreWithScores(key, max, min,
				offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
			String min, int offset, int count) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrevrangeByScoreWithScores(key, max, min,
				offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByScore(key, max, min, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max,
			String min) {
		Jedis jedis = getRead(key);
		Set<Tuple> ret = jedis.zrevrangeByScoreWithScores(key, max, min);
		jedis.close();

		return ret;
	}

	@Override
	public Long zremrangeByRank(String key, long start, long end) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zremrangeByRank(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long zremrangeByScore(String key, double start, double end) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zremrangeByScore(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long zremrangeByScore(String key, String start, String end) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zremrangeByScore(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long zunionstore(String dstkey, String... sets) {
		throw new NotSupportedException();
	}

	@Override
	public Long zunionstore(String dstkey, ZParams params, String... sets) {
		throw new NotSupportedException();
	}

	@Override
	public Long zinterstore(String dstkey, String... sets) {
		throw new NotSupportedException();
	}

	@Override
	public Long zinterstore(String dstkey, ZParams params, String... sets) {
		throw new NotSupportedException();
	}

	@Override
	public Long zlexcount(String key, String min, String max) {
		Jedis jedis = getRead(key);
		Long ret = jedis.zlexcount(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByLex(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrangeByLex(String key, String min, String max,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrangeByLex(key, min, max, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByLex(key, max, min);
		jedis.close();

		return ret;
	}

	@Override
	public Set<String> zrevrangeByLex(String key, String max, String min,
			int offset, int count) {
		Jedis jedis = getRead(key);
		Set<String> ret = jedis.zrevrangeByLex(key, max, min, offset, count);
		jedis.close();

		return ret;
	}

	@Override
	public Long zremrangeByLex(String key, String min, String max) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.zremrangeByLex(key, min, max);
		jedis.close();

		return ret;
	}

	@Override
	public Long strlen(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.strlen(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long lpushx(String key, String... string) {
		throw new NotSupportedException();
	}

	@Override
	public Long persist(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.persist(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long rpushx(String key, String... string) {
		throw new NotSupportedException();
	}

	@Override
	public String echo(String string) {
		Jedis jedis = getRead(string);
		String ret = jedis.echo(string);
		jedis.close();

		return ret;
	}

	@Override
	public Long linsert(String key, LIST_POSITION where, String pivot,
			String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.linsert(key, where, pivot, value);
		jedis.close();

		return ret;
	}

	@Override
	public String brpoplpush(String source, String destination, int timeout) {
		throw new NotSupportedException();
	}

	@Override
	public Boolean setbit(String key, long offset, boolean value) {
		Jedis jedis = getWrite(key);
		Boolean ret = jedis.setbit(key, offset, value);
		jedis.close();

		return ret;
	}

	@Override
	public Boolean setbit(String key, long offset, String value) {
		Jedis jedis = getWrite(key);
		Boolean ret = jedis.setbit(key, offset, value);
		jedis.close();

		return ret;
	}

	@Override
	public Boolean getbit(String key, long offset) {
		Jedis jedis = getWrite(key);
		Boolean ret = jedis.getbit(key, offset);
		jedis.close();

		return ret;
	}

	@Override
	public Long setrange(String key, long offset, String value) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.setrange(key, offset, value);
		jedis.close();

		return ret;
	}

	@Override
	public String getrange(String key, long startOffset, long endOffset) {
		Jedis jedis = getWrite(key);
		String ret = jedis.getrange(key, startOffset, endOffset);
		jedis.close();

		return ret;
	}

	@Override
	public Long bitpos(String key, boolean value) {
		Jedis jedis = getRead(key);
		Long ret = jedis.bitpos(key, value);
		jedis.close();

		return ret;
	}

	@Override
	public Long bitpos(String key, boolean value, BitPosParams params) {
		Jedis jedis = getRead(key);
		Long ret = jedis.bitpos(key, value, params);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> configGet(String pattern) {
		throw new NotSupportedException();
	}

	@Override
	public String configSet(String parameter, String value) {
		throw new NotSupportedException();
	}

	@Override
	public Object eval(String script, int keyCount, String... params) {
		throw new NotSupportedException();
	}

	@Override
	public void subscribe(JedisPubSub jedisPubSub, String... channels) {
		throw new NotSupportedException();
	}

	@Override
	public Long publish(String channel, String message) {
		throw new NotSupportedException();
	}

	@Override
	public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
		throw new NotSupportedException();
	}

	@Override
	public Object eval(String script, List<String> keys, List<String> args) {
		throw new NotSupportedException();
	}

	@Override
	public Object eval(String script) {
		throw new NotSupportedException();
	}

	@Override
	public Object evalsha(String script) {
		throw new NotSupportedException();
	}

	@Override
	public Object evalsha(String sha1, List<String> keys, List<String> args) {
		throw new NotSupportedException();
	}

	@Override
	public Object evalsha(String sha1, int keyCount, String... params) {
		throw new NotSupportedException();
	}

	@Override
	public Boolean scriptExists(String sha1) {
		throw new NotSupportedException();
	}

	@Override
	public List<Boolean> scriptExists(String... sha1) {
		throw new NotSupportedException();
	}

	@Override
	public String scriptLoad(String script) {
		throw new NotSupportedException();
	}

	@Override
	public List<Slowlog> slowlogGet() {
		throw new NotSupportedException();
	}

	@Override
	public List<Slowlog> slowlogGet(long entries) {
		throw new NotSupportedException();
	}

	@Override
	public Long objectRefcount(String string) {
		throw new NotSupportedException();
	}

	@Override
	public String objectEncoding(String string) {
		throw new NotSupportedException();
	}

	@Override
	public Long objectIdletime(String string) {
		throw new NotSupportedException();
	}

	@Override
	public Long bitcount(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.bitcount(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long bitcount(String key, long start, long end) {
		Jedis jedis = getRead(key);
		Long ret = jedis.bitcount(key, start, end);
		jedis.close();

		return ret;
	}

	@Override
	public Long bitop(BitOP op, String destKey, String... srcKeys) {
		throw new NotSupportedException();
	}

	@Override
	public List<Map<String, String>> sentinelMasters() {
		throw new NotSupportedException();
	}

	@Override
	public List<String> sentinelGetMasterAddrByName(String jedisName) {
		throw new NotSupportedException();
	}

	@Override
	public Long sentinelReset(String pattern) {
		throw new NotSupportedException();
	}

	@Override
	public List<Map<String, String>> sentinelSlaves(String jedisName) {
		throw new NotSupportedException();
	}

	@Override
	public String sentinelFailover(String jedisName) {
		throw new NotSupportedException();
	}

	@Override
	public String sentinelMonitor(String jedisName, String ip, int port,
			int quorum) {
		throw new NotSupportedException();
	}

	@Override
	public String sentinelRemove(String jedisName) {
		throw new NotSupportedException();
	}

	@Override
	public String sentinelSet(String jedisName, Map<String, String> parameterMap) {
		throw new NotSupportedException();
	}

	@Override
	public byte[] dump(String key) {
		Jedis jedis = getRead(key);
		byte[] ret = jedis.dump(key);
		jedis.close();

		return ret;
	}

	@Override
	public String restore(String key, int ttl, byte[] serializedValue) {
		Jedis jedis = getWrite(key);
		String ret = jedis.restore(key, ttl, serializedValue);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public Long pexpire(String key, int milliseconds) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.pexpire(key, milliseconds);
		jedis.close();

		return ret;
	}

	@Override
	public Long pexpire(String key, long milliseconds) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.pexpire(key, milliseconds);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public Long pexpireAt(String key, long millisecondsTimestamp) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.pexpireAt(key, millisecondsTimestamp);
		jedis.close();

		return ret;
	}

	@Override
	public Long pttl(String key) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.pttl(key);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public String psetex(String key, int milliseconds, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.psetex(key, milliseconds, value);
		jedis.close();

		return ret;
	}

	@Override
	public String psetex(String key, long milliseconds, String value) {
		Jedis jedis = getWrite(key);
		String ret = jedis.psetex(key, milliseconds, value);
		jedis.close();

		return ret;
	}

	@Override
	public String set(String key, String value, String nxxx) {
		Jedis jedis = getWrite(key);
		String ret = jedis.set(key, value, nxxx);
		jedis.close();

		return ret;
	}

	@Override
	public String set(String key, String value, String nxxx, String expx,
			int time) {
		Jedis jedis = getWrite(key);
		String ret = jedis.set(key, value, nxxx, expx, time);
		jedis.close();

		return ret;
	}

	@Override
	public String clientKill(String client) {
		throw new NotSupportedException();
	}

	@Override
	public String clientSetname(String name) {
		throw new NotSupportedException();
	}

	@Override
	public String migrate(String host, int port, String key, int destinationDb,
			int timeout) {
		throw new NotSupportedException();
	}

	@Override
	public ScanResult<String> scan(int cursor) {
		throw new NotSupportedException();
	}

	@Override
	public ScanResult<String> scan(int cursor, ScanParams params) {
		throw new NotSupportedException();
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		throw new NotSupportedException();
	}

	@Override
	@Deprecated
	public ScanResult<Entry<String, String>> hscan(String key, int cursor,
			ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<Entry<String, String>> ret = jedis
				.hscan(key, cursor, params);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public ScanResult<String> sscan(String key, int cursor) {
		Jedis jedis = getRead(key);
		ScanResult<String> ret = jedis.sscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public ScanResult<String> sscan(String key, int cursor, ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<String> ret = jedis.sscan(key, cursor, params);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public ScanResult<Tuple> zscan(String key, int cursor) {
		Jedis jedis = getRead(key);
		ScanResult<Tuple> ret = jedis.zscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	@Deprecated
	public ScanResult<Tuple> zscan(String key, int cursor, ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<Tuple> ret = jedis.zscan(key, cursor, params);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<String> scan(String cursor) {
		throw new NotSupportedException();
	}

	@Override
	public ScanResult<String> scan(String cursor, ScanParams params) {
		throw new NotSupportedException();
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
		Jedis jedis = getRead(key);
		ScanResult<Entry<String, String>> ret = jedis.hscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<Entry<String, String>> hscan(String key, String cursor,
			ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<Entry<String, String>> ret = jedis
				.hscan(key, cursor, params);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor) {
		Jedis jedis = getRead(key);
		ScanResult<String> ret = jedis.sscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<String> ret = jedis.sscan(key, cursor, params);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor) {
		Jedis jedis = getRead(key);
		ScanResult<Tuple> ret = jedis.zscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
		Jedis jedis = getRead(key);
		ScanResult<Tuple> ret = jedis.zscan(key, cursor);
		jedis.close();

		return ret;
	}

	@Override
	public String clusterNodes() {
		throw new NotSupportedException();
	}

	@Override
	public String clusterMeet(String ip, int port) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterReset(Reset resetType) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterAddSlots(int... slots) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterDelSlots(int... slots) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterInfo() {
		throw new NotSupportedException();
	}

	@Override
	public List<String> clusterGetKeysInSlot(int slot, int count) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterSetSlotNode(int slot, String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterSetSlotMigrating(int slot, String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterSetSlotImporting(int slot, String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterSetSlotStable(int slot) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterForget(String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterFlushSlots() {
		throw new NotSupportedException();
	}

	@Override
	public Long clusterKeySlot(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.clusterKeySlot(key);
		jedis.close();

		return ret;
	}

	@Override
	public Long clusterCountKeysInSlot(int slot) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterSaveConfig() {
		throw new NotSupportedException();
	}

	@Override
	public String clusterReplicate(String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> clusterSlaves(String nodeId) {
		throw new NotSupportedException();
	}

	@Override
	public String clusterFailover() {
		throw new NotSupportedException();
	}

	@Override
	public List<Object> clusterSlots() {
		throw new NotSupportedException();
	}

	@Override
	public String asking() {
		throw new NotSupportedException();
	}

	@Override
	public List<String> pubsubChannels(String pattern) {
		throw new NotSupportedException();
	}

	@Override
	public Long pubsubNumPat() {
		throw new NotSupportedException();
	}

	@Override
	public Map<String, String> pubsubNumSub(String... channels) {
		throw new NotSupportedException();
	}

	@Override
	public void close() {
		throw new NotSupportedException();
	}

	@Override
	public void setDataSource(Pool<Jedis> jedisPool) {
		throw new NotSupportedException();
	}

	@Override
	public Long pfadd(String key, String... elements) {
		Jedis jedis = getWrite(key);
		Long ret = jedis.pfadd(key, elements);
		jedis.close();

		return ret;
	}

	@Override
	public long pfcount(String key) {
		Jedis jedis = getRead(key);
		Long ret = jedis.pfcount(key);
		jedis.close();

		return ret;
	}

	@Override
	public long pfcount(String... keys) {
		throw new NotSupportedException();
	}

	@Override
	public String pfmerge(String destkey, String... sourcekeys) {
		throw new NotSupportedException();
	}

	@Override
	public List<String> blpop(int timeout, String key) {
		Jedis jedis = getWrite(key);
		List<String> ret = jedis.blpop(timeout, key);
		jedis.close();

		return ret;
	}

	@Override
	public List<String> brpop(int timeout, String key) {
		Jedis jedis = getWrite(key);
		List<String> ret = jedis.brpop(timeout, key);
		jedis.close();

		return ret;
	}

	public long decrByUntil0Lua(String key, long value) {
		Jedis jedis = getRead(key);
		long ret = doDecrByUntil0Lua(jedis, key, value);
		jedis.close();

		return ret;
	}

	/**
	 * Implemented by LUA. Minus a key by a value, then return the left value.
	 * If the left value is less than 0, return -1; if error, return -1.
	 * 
	 * @param key
	 *            the key of the redis variable.
	 * @param value
	 *            the value to minus off.
	 * @return the value left after minus. If it is less than 0, return -1; if
	 *         error, return -1.
	 */
	private long doDecrByUntil0Lua(Jedis jedis, String key, long value) {
		// If any error, return -1.
		if (value <= 0)
			return -1;

		// The logic is implemented in LUA script which is run in server thread,
		// which is single thread in one server.
		String script = " local leftvalue = redis.call('get', KEYS[1]); "
				+ " if ARGV[1] - leftvalue > 0 then return nil; else "
				+ " return redis.call('decrby', KEYS[1], ARGV[1]); end; ";

		Long leftValue = (Long) jedis.eval(script, 1, key, "" + value);

		// If the left value is less than 0, return -1.
		if (leftValue == null)
			return -1;

		return leftValue;
	}

	public long decrByUntil0Cas(String key, long value) {
		Jedis jedis = getRead(key);
		long ret = doDecrByUntil0Cas(jedis, key, value);
		jedis.close();

		return ret;
	}

	/**
	 * Implemented by CAS. Minus a key by a value, then return the left value.
	 * If the left value is less than 0, return -1; if error, return -1.
	 * 
	 * No synchronization, because redis client is not shared among multiple
	 * threads.
	 * 
	 * @param key
	 *            the key of the redis variable.
	 * @param value
	 *            the value to minus off.
	 * @return the value left after minus. If it is less than 0, return -1; if
	 *         error, return -1.
	 */
	private long doDecrByUntil0Cas(Jedis jedis, String key, long value) {
		// If any error, return -1.
		if (value <= 0)
			return -1;

		// Start the CAS operations.
		jedis.watch(key);

		// Start the transation.
		Transaction tx = jedis.multi();

		// Decide if the left value is less than 0, if no, terminate the
		// transation, return -1;
		String curr = tx.get(key).get();
		if (Long.valueOf(curr) - value < 0) {
			tx.discard();
			return -1;
		}

		// Minus the key by the value
		tx.decrBy(key, value);

		// Execute the transation and then handle the result
		List<Object> result = tx.exec();

		// If error, return -1;
		if (result == null || result.isEmpty()) {
			return -1;
		}

		// Extract the first result
		for (Object rt : result) {
			return Long.valueOf(rt.toString());
		}

		// The program never comes here.
		return -1;
	}

	public List<RedicNode> getRedicNodes() {
		return redicNodes;
	}

	public void setRedicNodes(List<RedicNode> redicNodes) {
		this.redicNodes = redicNodes;
	}

	public boolean isReadWriteSeparate() {
		return readWriteSeparate;
	}

	public void setReadWriteSeparate(boolean readWriteSeparate) {
		this.readWriteSeparate = readWriteSeparate;
	}

	public List<String> getNodeConnStrs() {
		return nodeConnStrs;
	}

	public void setNodeConnStrs(List<String> nodeConnStrs) {
		this.nodeConnStrs = nodeConnStrs;
	}
}

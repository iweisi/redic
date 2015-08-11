package com.robert.redis.redic;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class JedisClient {
	private Jedis jedis;

	public JedisClient(Jedis jedis) {
		this.jedis = jedis;
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
	public long decrByUntil0Lua(String key, long value) {
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
	public long decrByUntil0Cas(String key, long value) {
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

}

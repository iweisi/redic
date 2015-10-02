package com.robert.redis.redic.strategy;

import java.util.Date;
import java.util.Random;

public class RandomSelectStrategy implements SelectStrategy {
	private Random random = new Random(new Date().getTime());

	public int select(int count) {
		int i = random.nextInt(count);

		return i;
	}
}

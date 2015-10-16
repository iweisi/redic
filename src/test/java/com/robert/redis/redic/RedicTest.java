package com.robert.redis.redic;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@ContextConfiguration(locations = "/spring/application-context-redic-test.xml")
public class RedicTest extends AbstractTestNGSpringContextTests {
	@Test(groups = { "redic" })
	public void testRedic() {
		Redic redic = (Redic) applicationContext.getBean("redic");

		redic.set("name", "value");

		AssertJUnit.assertEquals("value", redic.get("name"));
	}
}

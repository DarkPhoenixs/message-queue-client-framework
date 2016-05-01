package org.darkphoenixs.kafka.core;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import org.junit.Assert;
import org.junit.Test;

public class ReflectionToolTest {

	@Test
	public void test() throws Exception {

		Assert.assertNotNull(new ReflectionTool());

		Assert.assertTrue(ReflectionTool.newInstance(DefaultDecoder.class,
				new VerifiableProperties()) instanceof Decoder);

		try {
			ReflectionTool.newInstance(TestInter.class);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof NoSuchMethodException);
		}

	}

}

abstract class TestInter {

	public TestInter() {
	}
}
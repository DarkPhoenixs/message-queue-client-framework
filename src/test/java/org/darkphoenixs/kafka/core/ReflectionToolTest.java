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

		try {
			ReflectionTool.newInstance(TestVO.class);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof NoSuchMethodException);
		}

		try {
			ReflectionTool.newInstance(TestVO.class, "test");

		} catch (Exception e) {
			Assert.assertTrue(e instanceof NoSuchMethodException);
		}

		try {
			ReflectionTool.newInstance(TestVO.class, "test", 0);

		} catch (Exception e) {
			Assert.assertTrue(e instanceof NoSuchMethodException);
		}

	}

}

class TestVO {

	private TestVO() {
	}

	public TestVO(String test) {

		new TestVO();
	}

	public TestVO(int test) {
		new TestVO();
	}
}

abstract class TestInter {

	public TestInter() {
	}
}
package org.darkphoenixs.activemq.util;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

public class PathUtilTest {

	@Test
	public void test() throws Exception {
		
		Assert.assertEquals("conf", PathUtil.CONF);
		Assert.assertEquals(System.getProperty("user.dir"), PathUtil.PATH);
		Assert.assertEquals(File.separator, PathUtil.SEPARATOR);
		System.out.println(PathUtil.CONF_PATH);
	}
}

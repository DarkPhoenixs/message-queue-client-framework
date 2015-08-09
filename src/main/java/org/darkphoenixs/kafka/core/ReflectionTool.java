/**
 * <p>Title: ReflectionTool.java</p>
 * <p>Description: ReflectionTool</p>
 * <p>Package: org.darkphoenixs.kafka.core</p>
 * <p>Company: www.github.com/DarkPhoenixs</p>
 * <p>Copyright: Dark Phoenixs (Open-Source Organization)</p>
 */
package org.darkphoenixs.kafka.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * <p>Title: ReflectionTool</p>
 * <p>Description: 反射工具类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class ReflectionTool {

	/**
	 * <p>Title: newInstance</p>
	 * <p>Description: 反射机制实例化对象</p>
	 *
	 * @param objClass 要实例化的类类型
	 * @param params 构造方法参数
	 * @return 实例化对象
	 */
	public static <T> T newInstance(Class<T> objClass, Object... params) {

		T t = null;

		Class<?>[] paramTypes = new Class<?>[params.length];

		for (int i = 0; i < params.length; i++)

			paramTypes[i] = params[i].getClass();

		try {
			Constructor<T> constructor = objClass.getConstructor(paramTypes);

			t = constructor.newInstance(params);

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		return t;
	}
}

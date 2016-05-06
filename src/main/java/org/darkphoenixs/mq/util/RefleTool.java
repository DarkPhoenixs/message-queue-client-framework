/*
 * Copyright 2015-2016 Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.darkphoenixs.mq.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * <p>Title: RefleTool</p>
 * <p>Description: 反射工具类</p>
 *
 * @since 2015-06-01
 * @author Victor.Zxy
 * @version 1.0
 */
public class RefleTool {

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

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return t;
	}
	
	/**
	 * <p>Title: getFieldValue</p>
	 * <p>Description: 反射机制获取属性值</p>
	 * 
	 * @param obj
	 * @param fieldName
	 * @param fieldType
	 * @return 属性值
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getFieldValue(Object obj, String fieldName, Class<T> fieldType) {
		
		Class<?> clasz = obj.getClass();

		for (; clasz != Object.class; clasz = clasz.getSuperclass()) {
			
			try {
				Field field = clasz.getDeclaredField(fieldName);
				
				field.setAccessible(true);
				
				return (T) field.get(obj);
				
			} catch (Exception e) {
				
			}
		}
		
		return null;
	}
}

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

import java.io.File;

/**
 * <p>Title: PathUtil</p>
 * <p>Description: 路径工具类</p>
 *
 * @author Victor.Zxy
 * @version 1.0
 * @since 2015-06-01
 */
public class PathUtil {

    /**
     * 路径分隔符
     */
    public static final String SEPARATOR = File.separator;

    /**
     * 当前路径
     */
    public static final String PATH = System.getProperty("user.dir");

    /**
     * 配置文件目录
     */
    public static final String CONF = "conf";

    /**
     * 配置文件路径
     */
    public static final String CONF_PATH = SEPARATOR + PATH + SEPARATOR + CONF
            + SEPARATOR;

}

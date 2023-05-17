package com.moyu.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xiaomingzhang
 * @date 2021/8/30
 * 读取properties文件
 */
public class PropertiesReadUtil {

    private static final Properties properties = new Properties();

    static {
        InputStream inputStream = Object.class.getResourceAsStream("/config.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("加载配置文件config.properties发生异常");
        }
    }


    public static String get(String key){
        return properties.getProperty(key);
    }


}

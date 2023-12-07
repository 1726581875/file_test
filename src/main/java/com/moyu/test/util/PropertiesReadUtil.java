package com.moyu.test.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xiaomingzhang
 * @date 2021/8/30
 * 读取properties文件
 */
public class PropertiesReadUtil {

    private static final Properties properties = new Properties();

    private static boolean isLoad = false;


    public static String get(String key) {
        if (!isLoad) {
            synchronized (PropertiesReadUtil.class) {
                if (!isLoad) {
                    loadConfig();
                }
            }
        }
        return properties.getProperty(key);
    }


    public static void loadConfig() {
        try {
            boolean isWindows = false;
            String osName = null;
            try {
                osName = System.getProperty("os.name", "未知");
                if(osName.startsWith("Windows")) {
                    isWindows = true;
                }
            } catch (SecurityException se) {
                isWindows = false;
                se.printStackTrace();
            }
            InputStream inputStream = null;
            System.out.println("osName:" + osName);
            if(isWindows) {
                inputStream = Object.class.getResourceAsStream("/config.properties");
            } else {
                System.out.println("非Windows读取jar包同目录下配置文件config.properties");
                String jarPath = PropertiesReadUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                System.out.println("当前类路径[" +  jarPath + "]");
                String jarDirPath = new File(jarPath).getParent();
                inputStream = new FileInputStream(jarDirPath + File.separator  + "config.properties");
            }
            properties.load(inputStream);
            isLoad = true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("加载配置文件config.properties发生异常");
            isLoad = false;
        }
    }


}

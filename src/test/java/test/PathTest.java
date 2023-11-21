package test;

import sun.misc.SharedSecrets;
import sun.misc.URLClassPath;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author xiaomingzhang
 * @date 2023/11/12
 */
public class PathTest {


    public static void main(String[] args) {
        ClassLoader classLoader = PathTest.class.getClassLoader();
        System.out.println(classLoader.getResource("").getPath());
        URLClassLoader parent = (URLClassLoader) classLoader.getParent();
        final URLClassPath ucp = SharedSecrets.getJavaNetAccess().getURLClassPath(parent);
        URL[] urLs = ucp.getURLs();
        for (URL url: urLs) {
            System.out.println(url.getPath());
        }
        //URLClassPath urlClassPath = new URLClassPath(null);
    }


}

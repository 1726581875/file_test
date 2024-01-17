package com.moyu.xmz.common.util;

import java.util.Collection;

/**
 * @author xiaomingzhang
 * @date 2023/11/8
 */
public class CollectionUtils {


    public static boolean isEmpty(Collection list){
        return list == null || list.size() == 0;
    }

    public static boolean isNotEmpty(Collection list){
        return !isEmpty(list);
    }


}

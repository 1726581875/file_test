package com.moyu.xmz.common.constant;

/**
 * @author xiaomingzhang
 * @date 2023/6/2
 */
public interface CommonConstant {

    byte PRIMARY_KEY = 1;
    byte GENERAL_INDEX = 2;
    byte UNIQUE_INDEX = 3;


    String JOIN_TYPE_INNER = "INNER";
    String JOIN_TYPE_LEFT = "LEFT";
    String JOIN_TYPE_RIGHT = "RIGHT";


    /**
     * 类似mysql的存储引擎，数据存储方式
     * yuStore 按简单顺序结构存储的(最初版本)
     * yanStore b+树叶子节点存储行
     */
    String ENGINE_TYPE_YU = "yuStore";
    String ENGINE_TYPE_YAN = "yanStore";

    int INT_LENGTH = 4;

    int LONG_LENGTH = 8;



}

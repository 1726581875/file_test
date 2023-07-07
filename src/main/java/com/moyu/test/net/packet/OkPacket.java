package com.moyu.test.net.packet;

import com.moyu.test.store.metadata.obj.SelectColumn;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class OkPacket extends Packet {

    /**
     * 操作类型
     * 0查找、1、新增、2、查找、3删除
     */
    private byte opType;
    /**
     * 受影响的行
     */
    private int affRows;
    /**
     * 查询结果行
     */
    private int resRows;

    private SelectColumn[] columns;

    private List<Object[]> resultRows;


}

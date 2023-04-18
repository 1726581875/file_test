package com.moyu.test.entiry;

import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/4/17
 */
public class DataContent {

    private DataHeader dataHeader;

    private Map<String, String> dataBody;

    public DataHeader getDataHeader() {
        return dataHeader;
    }

    public void setDataHeader(DataHeader dataHeader) {
        this.dataHeader = dataHeader;
    }

    public Map<String, String> getDataBody() {
        return dataBody;
    }

    public void setDataBody(Map<String, String> dataBody) {
        this.dataBody = dataBody;
    }
}

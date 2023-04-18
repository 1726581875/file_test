package com.moyu.test.entiry;

/**
 * @author xiaomingzhang
 * @date 2023/4/17
 */
public class DataHeader {

    private String fileType;

    private String totalRow;

    private long dataBodyStartPos;

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getTotalRow() {
        return totalRow;
    }

    public void setTotalRow(String totalRow) {
        this.totalRow = totalRow;
    }

    public long getDataBodyStartPos() {
        return dataBodyStartPos;
    }

    public void setDataBodyStartPos(long dataBodyStartPos) {
        this.dataBodyStartPos = dataBodyStartPos;
    }


    @Override
    public String toString() {
        return "DataHeader{" +
                "fileType='" + fileType + '\'' +
                ", totalRow='" + totalRow + '\'' +
                ", dataBodyStartPos=" + dataBodyStartPos +
                '}';
    }
}

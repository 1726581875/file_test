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


    public static DataHeader build(String headerStr) {
        DataHeader dataHeader = new DataHeader();
        String[] attributes = headerStr.split(";");
        for (String attribute : attributes) {
            String[] split = attribute.split("=");
            if ("fileType".equals(split[0])) {
                dataHeader.setFileType(split[1].trim());
            }
            if ("totalRow".equals(split[0])) {
                dataHeader.setTotalRow(split[1].trim());
            }
            if ("dataBodyStartPos".equals(split[0])) {
                dataHeader.setDataBodyStartPos(Long.valueOf(split[1].trim()));
            }
        }
        return dataHeader;
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

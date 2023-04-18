package com.moyu.test;

import com.moyu.test.entiry.DataHeader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaomingzhang
 * @date 2023/4/14
 * 随机读写文件
 */
public class RandomAccessFileTest {


    public static void main(String[] args) throws IOException {

        String filePath = "D:\\mytest\\fileTest\\xmz.yu";
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }

        int totalRow = 100;
        long dataBodyStartPos = 1024;

        // 写入文件
        String header = "fileType=xmz;totalRow=" + totalRow +";dataBodyStartPos=" + dataBodyStartPos;
        byte[] bytes = header.getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        RandomAccessFile randomAccessFile = new RandomAccessFile(filePath,"rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        System.out.println("FileChannel size:" + fileChannel.size());
        fileChannel.write(byteBuffer, 0);

        // 写入数据
        long pos = dataBodyStartPos;
        for (int i = 0 ; i < totalRow; i++) {
            String row = i + "," + "hello world\n";
            byte[] rowBytes = row.getBytes();
            ByteBuffer dataBuffer = ByteBuffer.wrap(rowBytes);
            fileChannel.write(dataBuffer, pos);
            pos = pos + rowBytes.length;
        }
        fileChannel.close();
        randomAccessFile.close();



        // 读取数据header
        RandomAccessFile readDataRAF = new RandomAccessFile(filePath,"rw");
        FileChannel readDataChannel = readDataRAF.getChannel();
        System.out.println("FileChannel size:" + readDataChannel.size());

        ByteBuffer headerBuff = ByteBuffer.allocate(200);
        readDataChannel.read(headerBuff, 0);
        String headerStr = new String(headerBuff.array(), "UTF-8");
        System.out.println(headerStr);

        DataHeader dataHeader = getDataHeader(headerStr);

        System.out.println("dataHeader=" + dataHeader);

        long readerPos = dataHeader.getDataBodyStartPos();
        ByteBuffer readerBuff = ByteBuffer.allocate(200);
        int len = 0;
        // 读取数据
        for (;len != -1;) {
            len = readDataChannel.read(readerBuff, readerPos);
            System.out.print(new String(readerBuff.array(), "UTF-8"));
            readerBuff = ByteBuffer.allocate(200);
            readerPos = readerPos + len;
        }
        readDataChannel.close();
        readDataRAF.close();
    }


    static DataHeader getDataHeader(String headerStr) {
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


}

package com.moyu.test;

import com.moyu.test.entiry.DataHeader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaomingzhang
 * @date 2023/4/18
 * 多线程读，互不影响
 */
public class MultiThreadReadFileTest {

    public static void main(String[] args) throws Exception {

        String filePath = "D:\\mytest\\fileTest\\xmz.yu";

        new Thread(new ReaderThread(filePath)).start();
        new Thread(new ReaderThread(filePath)).start();
        new Thread(new ReaderThread(filePath)).start();
        new Thread(new ReaderThread(filePath)).start();
        new Thread(new ReaderThread(filePath)).start();
    }


    static class ReaderThread implements Runnable {

        private String filePath;

        public ReaderThread(String filePath) {
            this.filePath = filePath;
        }

        public void run() {
            RandomAccessFile readDataRAF = null;
            FileChannel readDataChannel = null;
            try {
                // 读取数据header
                readDataRAF = new RandomAccessFile(filePath, "r");
                readDataChannel = readDataRAF.getChannel();
                ByteBuffer headerBuff = ByteBuffer.allocate(200);
                readDataChannel.read(headerBuff, 0);
                String headerStr = new String(headerBuff.array(), "UTF-8");
                DataHeader dataHeader = DataHeader.build(headerStr);
                long readerPos = dataHeader.getDataBodyStartPos();
                ByteBuffer readerBuff = ByteBuffer.allocate(200);
                int len = 0;
                // 读取数据
                while (len != -1) {
                    System.out.println("[" + Thread.currentThread().getName() + "]");
                    len = readDataChannel.read(readerBuff, readerPos);
                    System.out.print(new String(readerBuff.array(), "UTF-8"));
                    readerBuff = ByteBuffer.allocate(200);
                    readerPos = readerPos + len;
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (readDataChannel != null) {
                        readDataChannel.close();
                    }
                    if (readDataRAF != null) {
                        readDataRAF.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }


}

package com.moyu.test.store;

import com.moyu.test.exception.FileOperationException;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author xiaomingzhang
 * @date 2023/4/21
 */
public class FileStore {

    private String fileFullPath;

    private FileChannel fileChannel;

    private RandomAccessFile randomAccessFile;


    public FileStore(String fileFullPath) throws FileNotFoundException {
        this.fileFullPath = fileFullPath;
        randomAccessFile = new RandomAccessFile(fileFullPath, "rw");
        fileChannel = randomAccessFile.getChannel();
    }


    public ByteBuffer read(long pos, int len) {
        ByteBuffer dst = ByteBuffer.allocate(len);
        try {
            do {
                int l = fileChannel.read(dst, pos);
                if (l < 0) {
                    throw new EOFException();
                }
                pos += l;
            } while (dst.remaining() > 0);
            dst.rewind();
        } catch (IOException e) {
            e.printStackTrace();
            throw new FileOperationException("读文件发生异常");
        }
        return dst;
    }


    public void write(ByteBuffer byteBuffer, long startPos) {
        try {
            int off = 0;
            do {
                int len = fileChannel.write(byteBuffer, startPos + off);
                off += len;
            } while (byteBuffer.remaining() > 0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new FileOperationException("写文件发生异常");
        }
    }



    public void close() {
        try {
            fileChannel.close();
            randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}

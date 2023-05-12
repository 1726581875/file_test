package test.readwrite;

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
        long dataEndPos = 1024;

        // 写入文件头部信息
        String header = "fileType=xmz;totalRow=" + totalRow +";dataBodyStartPos=" + dataBodyStartPos + ";dataEndPos=" + dataEndPos;
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


        // 更新文件头部信息，记录文件结束位置dataEndPos
        dataEndPos = pos;
        writeHeader(filePath, totalRow, dataBodyStartPos, dataEndPos);


        // 读取数据header
        RandomAccessFile readDataRAF = new RandomAccessFile(filePath,"rw");
        FileChannel readDataChannel = readDataRAF.getChannel();
        System.out.println("FileChannel size:" + readDataChannel.size());

        ByteBuffer headerBuff = ByteBuffer.allocate(200);
        readDataChannel.read(headerBuff, 0);
        String headerStr = new String(headerBuff.array(), "UTF-8");
        System.out.println("headerStr=" + headerStr);
        DataHeader dataHeader = DataHeader.build(headerStr);
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


    private static void writeHeader(String filePath, int totalRow, long dataBodyStartPos, long dataEndPos) throws IOException {
        String header = "fileType=xmz;totalRow=" + totalRow +";dataBodyStartPos=" + dataBodyStartPos + ";dataEndPos=" + dataEndPos;
        byte[] bytes = header.getBytes();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        RandomAccessFile randomAccessFile = new RandomAccessFile(filePath,"rw");
        FileChannel fileChannel = randomAccessFile.getChannel();
        fileChannel.write(byteBuffer, 0);
        fileChannel.close();
        randomAccessFile.close();
    }


}

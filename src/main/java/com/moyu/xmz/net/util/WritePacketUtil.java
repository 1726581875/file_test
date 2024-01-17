package com.moyu.xmz.net.util;

import com.moyu.xmz.net.packet.OkPacket;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/10/9
 */
public class WritePacketUtil {

    public static void writeOkPacket(DataOutputStream out, OkPacket okPacket) throws IOException {
        byte[] bytes = okPacket.getBytes();
        out.writeInt(bytes.length);
        out.write(OkPacket.PACKET_TYPE_OK);
        out.write(bytes);
    }


}

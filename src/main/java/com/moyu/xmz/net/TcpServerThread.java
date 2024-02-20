package com.moyu.xmz.net;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.SqlParser;
import com.moyu.xmz.command.dml.SelectCmd;
import com.moyu.xmz.command.dml.sql.Parameter;
import com.moyu.xmz.net.model.terminal.DatabaseInfo;
import com.moyu.xmz.net.model.terminal.QueryResultDto;
import com.moyu.xmz.net.model.terminal.QueryResultStrDto;
import com.moyu.xmz.net.util.WritePacketUtil;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.net.constant.CmdTypeConstant;
import com.moyu.xmz.net.model.BaseResultDto;
import com.moyu.xmz.net.model.jdbc.PreparedParamDto;
import com.moyu.xmz.net.packet.ErrPacket;
import com.moyu.xmz.net.packet.OkPacket;
import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/5
 */
public class TcpServerThread implements Runnable {

    private Socket socket;

    private DataInputStream in;

    private DataOutputStream out;

    public TcpServerThread(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        try {
            // 读取命令类型
            Byte commandType = in.readByte();
            // 根据命令类型返回对应结果
            BaseResultDto resultDto = null;
            try {
                switch (commandType) {
                    case CmdTypeConstant.DB_INFO:
                        // 获取数据库名称
                        String databaseName = ReadWriteUtil.readString(in);
                        System.out.println("获取数据库信息，数据库名称:" + databaseName);
                        Database database = Database.getDatabase(databaseName);
                        resultDto = new DatabaseInfo(database);
                        break;
                    case CmdTypeConstant.DB_QUERY:
                        Integer databaseId = in.readInt();
                        Database dbObj = null;
                        String dbName = null;
                        // 数据库id为-1时候，执行不需要数据库的命令。例如show databases
                        if (new Integer(-1).equals(databaseId)) {
                            dbObj = null;
                        } else if (new Integer(-2).equals(databaseId)) {
                            // 如果数据库id传-2则表示要通过数据库名称来确认是哪个数据库
                            dbName = ReadWriteUtil.readString(in);
                            dbObj = Database.getDatabase(dbName);
                        } else {
                            dbObj = Database.getDatabase(databaseId);
                        }
                        // 获取待执行sql
                        String sql = ReadWriteUtil.readString(in);
                        // 执行sql并获取结果
                        resultDto = execSqlGetResult(sql, dbObj);
                        break;
                    case CmdTypeConstant.DB_QUERY_RES_STR:
                        Integer databaseId2 = in.readInt();
                        Database dbObj2 = null;
                        String dbName2 = null;
                        // 数据库id为-1时候，执行不需要数据库的命令。例如show databases
                        if (new Integer(-1).equals(databaseId2)) {
                            dbObj2 = null;
                        } else if (new Integer(-2).equals(databaseId2)) {
                            // 如果数据库id传-2则表示要通过数据库名称来确认是哪个数据库
                            dbName2 = ReadWriteUtil.readString(in);
                            dbObj2 = Database.getDatabase(dbName2);
                        } else {
                            dbObj2 = Database.getDatabase(databaseId2);
                        }
                        // 获取待执行sql
                        String sql2 = ReadWriteUtil.readString(in);
                        // 结果字符格式类型，0横向表格，1竖向表格
                        byte formatType = in.readByte();
                        // 执行sql并获取结果
                        QueryResultDto queryResultDto = execSqlGetResult(sql2, dbObj2);
                        // 按格式拼接结果字符串
                        String formatResult = PrintResultUtil.getFormatResult(queryResultDto, formatType,-1);
                        if(queryResultDto.getDesc() != null) {
                            formatResult += queryResultDto.getDesc() + "\n";
                        }
                        formatResult += "\n";
                        System.out.println(formatResult);
                        resultDto = new QueryResultStrDto(formatResult);
                        break;
                    case CmdTypeConstant.DB_QUERY_PAGE:
                        handleQueryPage();
                        return;
                    case CmdTypeConstant.DB_PREPARED_QUERY:
                        resultDto = preparedQuery();
                        break;
                    default:
                        ExceptionUtil.throwDbException("内容类型不合法,commandType:{}", commandType);
                }
                // 发送结果包
                OkPacket okPacket = new OkPacket(1, resultDto, commandType);
                byte[] bytes = okPacket.getBytes();
                out.writeInt(bytes.length);
                out.write(OkPacket.PACKET_TYPE_OK);
                out.write(bytes);
            } catch (Throwable t) {
                t.printStackTrace();
                String errMsg = t.getMessage() == null ? t.toString() : t.getMessage();
                ErrPacket errPacket = new ErrPacket(1, errMsg);
                byte[] bytes = errPacket.getBytes();
                out.writeInt(bytes.length);
                out.write(OkPacket.PACKET_TYPE_ERR);
                out.write(bytes);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            try {
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    private void handleQueryPage() throws IOException {
        Integer databaseId = in.readInt();
        Database dbObj = Database.getDatabase(databaseId);
        // 获取待执行sql
        String sql = ReadWriteUtil.readString(in);
        System.out.println("数据库id:" + dbObj.getDatabaseId() + "数据库:" + dbObj.getDbName() + "接收到SQL:" + sql);
        ConnectSession connectSession = new ConnectSession(dbObj);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        if (command instanceof SelectCmd) {
            SelectCmd selectCmd = (SelectCmd) command;
            // 获取一页数据
            QueryResult pageResult = selectCmd.getNextPageResult();
            // 数据转换
            QueryResultDto queryResultDto = QueryResultDto.valueOf(pageResult);
            OkPacket okPacket = new OkPacket(0, queryResultDto, CmdTypeConstant.DB_QUERY_PAGE);
            // 数据发送
            WritePacketUtil.writeOkPacket(out, okPacket);
            while (pageResult.getHasNext() == (byte) 1) {
                byte flag = in.readByte();
                if (flag == (byte) 0) {
                    break;
                }
                System.out.println(Thread.currentThread().getName() + "获取下一批数据, flag=" + flag);
                pageResult = selectCmd.getNextPageResult();
                // 数据转换
                queryResultDto = QueryResultDto.valueOf(pageResult);
                okPacket = new OkPacket(0, queryResultDto, CmdTypeConstant.DB_QUERY_PAGE);
                // 数据发送
                WritePacketUtil.writeOkPacket(out, okPacket);
            }
        }
    }



    private QueryResultDto execSqlGetResult(String sql, Database dbObj) {
        if(dbObj != null) {
            System.out.println("数据库id:" + dbObj.getDatabaseId() + "数据库:" + dbObj.getDbName() + "接收到SQL:" + sql);
        } else {
            System.out.println("接收到SQL:" + sql);
        }
        ConnectSession connectSession = new ConnectSession(dbObj);
        // sql解析
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        // 执行sql并且获取执行结果
        QueryResult queryResult = command.exec();
        QueryResultDto queryResultDto = QueryResultDto.valueOf(queryResult);
        return queryResultDto;
    }


    /**
     * 预编译查询
     * @return
     * @throws IOException
     */
    private QueryResultDto preparedQuery() throws Exception {
        String dbName = ReadWriteUtil.readString(in);
        Database dbObj = Database.getDatabase(dbName);
        // 获取待执行sql
        String sql = ReadWriteUtil.readString(in);
        System.out.println("数据库id:" + dbObj.getDatabaseId() + "数据库:" + dbObj.getDbName() + "接收到SQL:" + sql);
        ConnectSession connectSession = new ConnectSession(dbObj);
        // 获取查询参数
        int paramPackLen = in.readInt();
        byte[] bytes = new byte[paramPackLen];
        in.readFully(bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        PreparedParamDto paramDto = new PreparedParamDto(byteBuffer);
        // 转换为预编译参数
        List<Parameter> queryParams = new ArrayList<>();
        StringBuilder paramPrintStr = new StringBuilder("");
        if(paramDto.getSize() > 0) {
            for (int i = 0; i < paramDto.getSize(); i++) {
                int paramIndex = i + 1;
                queryParams.add(new Parameter(paramIndex, paramDto.getValueArr()[i]));
                paramPrintStr.append("("+paramIndex +")[" + paramDto.getValueArr()[i] + "]");
                if(i != paramDto.getSize() - 1) {
                    paramPrintStr.append(", ");
                }
            }
        }
        System.out.println("参数：" + paramPrintStr.toString());
        AbstractCmd command = (AbstractCmd) connectSession.prepareCommand(sql);
        command.setParameterValues(queryParams);
        QueryResult queryResult = command.exec();
        QueryResultDto queryResultDto = QueryResultDto.valueOf(queryResult);
        return queryResultDto;
    }
}

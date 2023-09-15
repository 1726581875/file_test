package com.moyu.test.net;

import com.moyu.test.command.Command;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.SqlParser;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.net.constant.CommandTypeConstant;
import com.moyu.test.net.model.BaseResultDto;
import com.moyu.test.net.model.terminal.ColumnDto;
import com.moyu.test.net.model.terminal.DatabaseInfo;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.model.terminal.RowValueDto;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.net.util.ReadWriteUtil;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
                    case CommandTypeConstant.DB_INFO:
                        // 获取数据库名称
                        String databaseName = ReadWriteUtil.readString(in);
                        System.out.println("获取数据库信息，数据库名称:" + databaseName);
                        Database database = Database.getDatabase(databaseName);
                        resultDto = new DatabaseInfo(database);
                        break;
                    case CommandTypeConstant.DB_QUERY:
                    case CommandTypeConstant.SHOW_DATABASE:
                        Integer databaseId = in.readInt();;
                        Database dbObj = null;
                        // 数据库id为-1时候，执行不需要数据库的命令。例如show databases
                        if(!new Integer(-1).equals(databaseId)) {
                            dbObj = Database.getDatabase(databaseId);
                        }
                        // 获取待执行sql
                        String sql = ReadWriteUtil.readString(in);
                        System.out.println("数据库id:" + databaseId + "接收到SQL:" + sql);
                        ConnectSession connectSession = new ConnectSession(dbObj);
                        // sql解析
                        SqlParser sqlParser = new SqlParser(connectSession);
                        Command command = sqlParser.prepareCommand(sql);
                        // 执行sql并且获取执行结果
                        QueryResult queryResult = command.execCommand();
                        QueryResultDto queryResultDto = queryResultToDTO(queryResult);
                        resultDto = queryResultDto;
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
            } catch (Exception e) {
                e.printStackTrace();
                String errMsg = e.getMessage() == null ? e.toString() : e.getMessage();
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

    private QueryResultDto queryResultToDTO(QueryResult queryResult) {
        SelectColumn[] selectColumns = queryResult.getSelectColumns();
        // 字段信息转换
        ColumnDto[] columnDtos = new ColumnDto[selectColumns.length];
        for (int i = 0; i < columnDtos.length; i++) {
            String columnName = selectColumns[i].getSelectColumnName();
            String alias = selectColumns[i].getAlias();
            String tableAlias = selectColumns[i].getTableAlias();
            byte columnType;
            Column column = selectColumns[i].getColumn();
            // 如果没有指数据库字段对象，默认为字符类型
            if(selectColumns[i].getColumnType() == null) {
                if (column == null) {
                    columnType = ColumnTypeConstant.VARCHAR;
                } else {
                    columnType = column.getColumnType();
                }
            } else {
                columnType = selectColumns[i].getColumnType();
            }
            columnDtos[i] = new ColumnDto(columnName, alias, tableAlias, columnType);
        }

        // 查询结果行转换
        RowValueDto[] rowValueDtos = null;
        List<Object[]> resultRows = queryResult.getResultRows();
        if (resultRows != null && resultRows.size() > 0) {
            rowValueDtos = new RowValueDto[resultRows.size()];
            for (int i = 0; i < resultRows.size(); i++) {
                rowValueDtos[i] = new RowValueDto(resultRows.get(i));
            }
        }
        return new QueryResultDto(columnDtos, rowValueDtos, queryResult.getDesc());
    }


}

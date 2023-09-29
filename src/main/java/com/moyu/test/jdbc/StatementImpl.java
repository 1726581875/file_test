package com.moyu.test.jdbc;

import com.moyu.test.exception.DbException;
import com.moyu.test.jdbc.util.ReadPacketUtil;
import com.moyu.test.net.constant.CommandTypeConstant;
import com.moyu.test.net.model.BaseResultDto;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.net.packet.Packet;
import com.moyu.test.net.util.ReadWriteUtil;
import java.io.*;
import java.net.Socket;
import java.sql.*;

/**
 * @author xiaomingzhang
 * @date 2023/9/18
 */
public class StatementImpl implements Statement {

    private ConnectionImpl conn;

    public StatementImpl(ConnectionImpl conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        Integer databaseId = conn.getDatabaseInfo().getDatabaseId();
        QueryResultDto queryResultDto = execQueryGetResult(databaseId, sql);
        if(queryResultDto == null) {
            throw new DbException("执行查询失败");
        }
        return new ResultSetImpl(queryResultDto);
    }


    public QueryResultDto execQueryGetResult(Integer databaseId, String sql) {
        // 获取结果
        Packet packet = execQueryGetPacket(databaseId, sql);
        if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
            OkPacket okPacket = (OkPacket) packet;
            BaseResultDto content = okPacket.getContent();
            return (QueryResultDto) content;
        } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
            ErrPacket errPacket = (ErrPacket) packet;
            System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
        } else {
            System.out.println("不支持的packet type" + packet.getPacketType());
        }
        return null;
    }

    public Packet execQueryGetPacket(Integer databaseId, String sql) {
        // 创建Socket对象，并指定服务端IP地址和端口号
        try (Socket socket = conn.getSocket();
             // 获取输入流和输出流
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            // 命令类型
            dataOutputStream.writeByte(CommandTypeConstant.DB_QUERY);
            // 数据库id
            dataOutputStream.writeInt(databaseId);
            // SQL
            ReadWriteUtil.writeString(dataOutputStream, sql);
            // 获取结果
            Packet packet = ReadPacketUtil.readPacket(dataInputStream);
            return packet;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



    @Override
    public int executeUpdate(String sql) throws SQLException {
        Integer databaseId = conn.getDatabaseInfo().getDatabaseId();
        Packet packet = execQueryGetPacket(databaseId, sql);
        if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
            OkPacket okPacket = (OkPacket) packet;
            return okPacket.getAffRows();
        } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
            ErrPacket errPacket = (ErrPacket) packet;
            System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
        } else {
            System.out.println("不支持的packet type" + packet.getPacketType());
        }
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        Integer databaseId = conn.getDatabaseInfo().getDatabaseId();
        // 获取结果
        Packet packet = execQueryGetPacket(databaseId, sql);
        if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
            return true;
        } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
            ErrPacket errPacket = (ErrPacket) packet;
            System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
        } else {
            System.out.println("不支持的packet type" + packet.getPacketType());
        }
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}

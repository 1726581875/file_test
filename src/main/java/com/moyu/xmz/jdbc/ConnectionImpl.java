package com.moyu.xmz.jdbc;

import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.jdbc.util.ReadPacketUtil;
import com.moyu.xmz.net.constant.CommandTypeConstant;
import com.moyu.xmz.net.model.terminal.DatabaseInfo;
import com.moyu.xmz.net.packet.ErrPacket;
import com.moyu.xmz.net.packet.OkPacket;
import com.moyu.xmz.net.packet.Packet;

import java.io.*;
import java.net.Socket;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author xiaomingzhang
 * @date 2023/9/18
 */
public class ConnectionImpl implements Connection {

    private String ipAddress;

    private Integer port;

    private String dbName;

    private DatabaseInfo databaseInfo;

    public ConnectionImpl(String url) {
        String[] split = url.split(":");
        this.ipAddress = split[0];
        this.port = Integer.valueOf(split[1]);
        this.dbName = split[2];
    }

    public DatabaseInfo getDatabaseInfo() {
        if (databaseInfo != null) {
            return databaseInfo;
        }
        this.databaseInfo = getDatabaseInfo(dbName);
        return databaseInfo;
    }


    public DatabaseInfo getDatabaseInfo(String databaseName) {
        // 创建Socket对象，并指定服务端IP地址和端口号
        try (Socket socket = new Socket(ipAddress, port);
             // 获取输入流和输出流
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            // 命令类型
            dataOutputStream.writeByte(CommandTypeConstant.DB_INFO);
            // 数据库名称
            ReadWriteUtil.writeString(dataOutputStream, databaseName);
            // 获取结果
            Packet packet = ReadPacketUtil.readPacket(dataInputStream);
            if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
                OkPacket okPacket = (OkPacket) packet;
                return (DatabaseInfo) okPacket.getContent();
            } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
                ErrPacket errPacket = (ErrPacket) packet;
                System.out.println("获取数据库信息失败, 错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
            } else {
                System.out.println("不支持的packet type" + packet.getPacketType());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new DbException("获取数据库信息失败");
    }

    public Socket getSocket() throws IOException {
        return new Socket(ipAddress, port);
    }


    @Override
    public Statement createStatement() throws SQLException {
        return new StatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatementImpl(sql, this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {

    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public String getDbName() {
        return dbName;
    }
}

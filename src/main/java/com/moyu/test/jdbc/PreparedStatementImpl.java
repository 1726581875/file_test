package com.moyu.test.jdbc;

import com.moyu.test.command.dml.sql.Parameter;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.jdbc.util.ReadPacketUtil;
import com.moyu.test.net.constant.CommandTypeConstant;
import com.moyu.test.net.model.BaseResultDto;
import com.moyu.test.net.model.jdbc.PreparedParamDto;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.net.packet.Packet;
import com.moyu.test.net.util.ReadWriteUtil;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Socket;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/9/18
 */
public class PreparedStatementImpl implements PreparedStatement {

    private String sql;

    private ConnectionImpl conn;
    /**
     * 参数位置,参数值
     */
    private Map<Integer, Parameter> parameterMap = new HashMap<>();
    /**
     * 参数位置，参数类型
     */
    private Map<Integer, Byte> parameterType = new HashMap<>();

    public PreparedStatementImpl(String sql, ConnectionImpl conn) {
        this.sql = sql;
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        QueryResultDto queryResultDto = execQueryGetResult(sql);
        if(queryResultDto == null) {
            throw new SqlExecutionException("查询结果为空");
        }
        return new ResultSetImpl(queryResultDto);
    }


    public QueryResultDto execQueryGetResult(String sql) {
        // 获取结果
        Packet packet = execQueryGetPacket(conn.getDbName(), sql);
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

    public Packet execQueryGetPacket(String databaseName, String sql) {
        // 创建Socket对象，并指定服务端IP地址和端口号
        try (Socket socket = conn.getSocket();
             // 获取输入流和输出流
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            // 命令类型
            dataOutputStream.writeByte(CommandTypeConstant.DB_PREPARED_QUERY);
            // 数据库名称
            ReadWriteUtil.writeString(dataOutputStream, databaseName);
            // SQL
            ReadWriteUtil.writeString(dataOutputStream, sql);
            // 参数
            byte[] paramTypes = new byte[parameterMap.size()];
            Object[] paramValues = new Object[parameterMap.size()];
            for (int i = 0; i < parameterMap.size(); i++) {
                int index = i + 1;
                Parameter parameter = parameterMap.get(index);
                if(parameter == null) {
                    ExceptionUtil.throwSqlQueryException("缺少第{}位参数", index);
                }
                if(parameterType.get(index) != null) {
                    paramTypes[i] = parameterType.get(index);
                } else {
                    paramTypes[i] = getValueType(parameter.getValue());
                }
                paramValues[i] = convertValueType(parameter.getValue());
            }
            PreparedParamDto paramDto = new PreparedParamDto(paramTypes, paramValues);
            ByteBuffer byteBuffer = paramDto.getByteBuffer();
            // 参数总字节长度
            dataOutputStream.writeInt(byteBuffer.limit());
            while (byteBuffer.remaining() > 0) {
                dataOutputStream.write(byteBuffer.get());
            }
            // 获取结果
            Packet packet = ReadPacketUtil.readPacket(dataInputStream);
            return packet;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 对一些特殊类型进行转换
     * @param value
     * @return
     */
    private Object convertValueType(Object value) {
        if (value == null) {
            return value;
        }
        if (value instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
        } else {
            return value;
        }
    }


    private byte getValueType(Object value) {

        if (value == null) {
            return -1;
        }
        if (value instanceof Integer) {
            return ColumnTypeConstant.INT_4;
        } else if (value instanceof Long) {
            return ColumnTypeConstant.INT_8;
        } else if (value instanceof BigInteger) {
            return ColumnTypeConstant.UNSIGNED_INT_8;
        } else if (value instanceof String) {
            return ColumnTypeConstant.VARCHAR;
        } else if (value instanceof java.util.Date) {
            return ColumnTypeConstant.TIMESTAMP;
        } else if (value instanceof LocalDateTime) {
            return ColumnTypeConstant.TIMESTAMP;
        } else if (value instanceof Byte) {
            return ColumnTypeConstant.TINY_INT;
        } else {
            throw new IllegalArgumentException("不支持类型" + value.getClass().getName());
        }
    }



    @Override
    public int executeUpdate() throws SQLException {
        // 获取结果
        Packet packet = execQueryGetPacket(conn.getDbName(), sql);
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
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {

    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {

    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {

    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {

    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {

    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {

    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {

    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void clearParameters() throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        parameterMap.put(parameterIndex, new Parameter(parameterIndex, x));
    }

    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public ResultSetMetaDataImpl getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {

    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
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

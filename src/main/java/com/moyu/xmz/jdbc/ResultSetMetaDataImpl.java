package com.moyu.xmz.jdbc;
import com.moyu.xmz.net.model.terminal.ColumnMetaDto;
import com.moyu.xmz.net.model.terminal.QueryResultDto;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author xiaomingzhang
 * @date 2023/9/18
 */
public class ResultSetMetaDataImpl implements ResultSetMetaData {

    private QueryResultDto queryResultDto;

    private ColumnMetaDto[] metaDataColumns;


    public ResultSetMetaDataImpl(QueryResultDto queryResultDto) {
        this.queryResultDto = queryResultDto;
        this.metaDataColumns = queryResultDto.getColumns();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return metaDataColumns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        String alias = metaDataColumns[column - 1].getAlias();
        if(alias != null && alias.length() > 0) {
            return alias;
        }
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return metaDataColumns[column - 1].getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
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

package com.moyu.xmz.net.model.terminal;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.net.model.BaseResultDto;
import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.store.common.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class QueryResultDto implements BaseResultDto {

    private int totalByteLen;

    private int columnsLen;

    private int rowsLen;

    private byte hasNext;

    private ColumnMetaDto[] columns;

    private RowDto[] rows;

    /**
     * 描述
     */
    private String desc;



    public QueryResultDto(ColumnMetaDto[] columns, RowDto[] rows, String desc) {
        this.totalByteLen = 0;
        this.columnsLen = columns.length;
        this.columns = columns;
        if (rows == null) {
            this.rowsLen = 0;
        } else {
            this.rowsLen = rows.length;
        }
        this.rows = rows;
        this.desc = desc;
    }


    public static QueryResultDto valueOf(QueryResult queryResult) {
        SelectColumn[] selectColumns = queryResult.getSelectColumns();
        // 字段信息转换
        ColumnMetaDto[] columnDtos = new ColumnMetaDto[selectColumns.length];
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
            columnDtos[i] = new ColumnMetaDto(columnName, alias, tableAlias, columnType);
        }

        // 查询结果行转换
        RowDto[] rowValueDtos = new RowDto[0];
        List<Object[]> resultRows = queryResult.getResultRows();
        if (resultRows != null && resultRows.size() > 0) {
            rowValueDtos = new RowDto[resultRows.size()];
            for (int i = 0; i < resultRows.size(); i++) {
                rowValueDtos[i] = new RowDto(resultRows.get(i));
            }
        }
        QueryResultDto queryResultDto = new QueryResultDto(columnDtos, rowValueDtos, queryResult.getDesc());
        queryResultDto.setHasNext(queryResult.getHasNext());
        return queryResultDto;
    }



    public QueryResultDto(ByteBuffer byteBuffer) {
        this.totalByteLen = byteBuffer.getInt();
        this.columnsLen = byteBuffer.getInt();
        this.rowsLen = byteBuffer.getInt();
        this.hasNext = byteBuffer.get();
        this.columns = new ColumnMetaDto[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            this.columns[i] = new ColumnMetaDto(byteBuffer);
        }
        this.rows = new RowDto[rowsLen];
        for (int i = 0; i < rowsLen; i++) {
            this.rows[i] = new RowDto(byteBuffer, this.columns);
        }
        this.desc = ReadWriteUtil.readString(byteBuffer);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(totalByteLen);
        writeBuffer.putInt(columnsLen);
        writeBuffer.putInt(rowsLen);
        writeBuffer.put(this.hasNext);
        for (ColumnMetaDto columnDto : columns) {
            writeBuffer.put(columnDto.getByteBuffer());
        }
        if (rowsLen > 0) {
            for (RowDto rowValueDto : rows) {
                writeBuffer.put(rowValueDto.getByteBuffer(columns));
            }
        }
        ReadWriteUtil.writeString(writeBuffer, desc);
        totalByteLen = writeBuffer.position();
        writeBuffer.putInt(0, totalByteLen);
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }


    public QueryResultDto(int totalByteLen, int columnsLen, int rowsLen, ColumnMetaDto[] columns, RowDto[] rows, String desc) {
        this.totalByteLen = totalByteLen;
        this.columnsLen = columnsLen;
        this.rowsLen = rowsLen;
        this.columns = columns;
        this.rows = rows;
        this.desc = desc;
    }


    public ColumnMetaDto[] getColumns() {
        return columns;
    }

    public RowDto[] getRows() {
        return rows;
    }

    public String getDesc() {
        return desc;
    }

    public int getColumnsLen() {
        return columnsLen;
    }

    public int getRowsLen() {
        return rowsLen;
    }

    public void setHasNext(byte hasNext) {
        this.hasNext = hasNext;
    }

    public byte getHasNext() {
        return hasNext;
    }

    @Override
    public String toString() {
        return "QueryResultDto{" +
                "totalByteLen=" + totalByteLen +
                ", columnsLen=" + columnsLen +
                ", rowsLen=" + rowsLen +
                ", columns=" + Arrays.toString(columns) +
                ", rows=" + Arrays.toString(rows) +
                ", desc='" + desc + '\'' +
                '}';
    }

}

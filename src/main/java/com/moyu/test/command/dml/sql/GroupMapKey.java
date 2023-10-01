package com.moyu.test.command.dml.sql;

import com.moyu.test.store.metadata.obj.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/6
 */
public class GroupMapKey {


    private List<Column> columnList;

    public GroupMapKey(List<Column> columnList) {
        this.columnList = columnList;
    }


    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupMapKey that = (GroupMapKey) o;
        List<Column> thatColumnList = that.getColumnList();
        for (int i = 0; i < this.columnList.size(); i++) {
            if (!this.columnList.get(i).equals(thatColumnList.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}

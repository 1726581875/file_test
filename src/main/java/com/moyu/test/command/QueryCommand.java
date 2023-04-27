package com.moyu.test.command;

import com.moyu.test.command.condition.Condition;
import com.moyu.test.store.Chunk;
import com.moyu.test.store.UnfixedLengthStore;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class QueryCommand implements Command {

    private String fileName;

    private Integer limit;

    private Integer offset;

    /**
     * todo 搞简单单个条件查询
     */
    private Condition condition;

    public QueryCommand(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public List<Chunk> execute() {
        List<Chunk> resultChunk = new ArrayList<>();
        UnfixedLengthStore unfixedLenStore = null;
        try {
            unfixedLenStore = new UnfixedLengthStore(fileName);
            Chunk chunk = null;
            int index = 0;
            while ((chunk = unfixedLenStore.getNextChunk()) != null) {
                if(isEligibleData(chunk, index)) {
                    resultChunk.add(chunk);
                }
                index++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (unfixedLenStore != null) {
                unfixedLenStore.close();
            }
        }
        return resultChunk;
    }


    /**
     * 刷选符合条件的数据
     * @param chunk
     * @param index
     * @return
     */
    private boolean isEligibleData(Chunk chunk, int index) {
        // todo 根据条件配置判断数据是否符合条件

        return true;
    }


    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }
}

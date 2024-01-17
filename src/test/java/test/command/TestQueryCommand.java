package test.command;

import com.moyu.xmz.common.constant.OperatorConstant;
import test.readwrite.UnfixedLengthStore;
import test.readwrite.entity.Chunk;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class TestQueryCommand {

    private String fileName;

    private Integer limit;

    private Integer offset;

    /**
     * todo 搞简单单个条件查询
     */
    private Condition condition;

    public TestQueryCommand(String fileName) {
        this.fileName = fileName;
    }

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
        boolean result = true;

        // 是否满足offset、limit的行，不满足标记为false
        if (offset != null && limit != null) {
            int beginIndex = offset;
            int endIndex = offset + limit - 1;
            if (index < beginIndex || index > endIndex) {
                return false;
            }
        }

        // todo 当前条件只是简单单个条件,更复杂场景先不考虑
        if (condition != null) {
            boolean conditionResult = getConditionResult(condition, chunk);
            if(!conditionResult) {
                result = false;
            }
        }
        return result;
    }


    private boolean getConditionResult(Condition condition, Chunk chunk) {
        boolean result = false;
        List<String> conditionValueList = condition.getValue();
        switch (condition.getOperator()) {
            case OperatorConstant.EQUAL:
                if(chunk.getData() != null && chunk.getData().equals(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
                if(chunk.getData() != null && !chunk.getData().equals(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.LIKE:
                if(chunk.getData() != null && chunk.getData().contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_LIKE:
                if(chunk.getData() != null && !chunk.getData().contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.IN:
                Set<String> valueSet = conditionValueList.stream().collect(Collectors.toSet());
                if(chunk.getData() != null && valueSet.contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.NOT_IN:
                Set<String> valueSet2 = conditionValueList.stream().collect(Collectors.toSet());
                if(chunk.getData() != null && !valueSet2.contains(conditionValueList.get(0))){
                    result = true;
                }
                break;
            case OperatorConstant.IS_NULL:
                if(chunk.getData() == null) {
                    result = true;
                }
                break;
            case OperatorConstant.IS_NOT_NULL:
                if(chunk.getData() != null) {
                    result = true;
                }
                break;
            case OperatorConstant.EXISTS:
                // todo
                break;
            case OperatorConstant.NOT_EXISTS:
                // todo
                break;
            default:
                throw new UnsupportedOperationException("不支持操作符:" + condition.getOperator());
        }

        return result;
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

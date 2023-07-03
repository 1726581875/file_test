package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.store.operation.BasicOperation;
import com.moyu.test.store.operation.OperateTableInfo;
import com.moyu.test.util.PathUtil;

/**
 * @author xiaomingzhang
 * @date 2023/5/18
 */
public class DeleteCommand extends AbstractCommand {

    private OperateTableInfo tableInfo;

    public DeleteCommand(OperateTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public String execute() {
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        int deleteRowNum = engineOperation.delete();
        return "共删除了" + deleteRowNum + "行数据";
    }



    private int deleteAllData(DataChunkStore dataChunkStore) {
        int dataChunkNum = dataChunkStore.getDataChunkNum();
        int deleteRowNum = 0;
        // 遍历数据块
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            deleteRowNum += chunk.getRowNum();
            // 清空块内所有行数据
            chunk.clear();
            // 更新块整个数据块到磁盘
            dataChunkStore.updateChunk(chunk);

        }

        // 清空所有索引
        if (tableInfo.getIndexList() != null && tableInfo.getIndexList().size() > 0) {
            for (IndexMetadata index : tableInfo.getIndexList()) {
                //Column indexColumn = getIndexColumn(index, columns);
                String indexPath = PathUtil.getIndexFilePath(tableInfo.getSession().getDatabaseId(), tableInfo.getTableName(), index.getIndexName());
                BpTreeMap<Comparable, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Comparable.class);
                bpTreeMap.clear();
            }
        }
        return deleteRowNum;
    }

}

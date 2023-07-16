package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class ShowTablesCommand extends AbstractCommand {

    private Integer databaseId;

    private List<TableMetadata> resultList = new ArrayList<>();


    public ShowTablesCommand(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String[] getAllTable() {
        List<String> list = new ArrayList<>();
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(databaseId);
            List<TableMetadata> allData = metadataStore.getCurrDbAllTable();

            for (int i = 0; i < allData.size(); i++) {
                list.add(allData.get(i).getTableName() + "  |  " + allData.get(i).getTableId());
            }
            resultList = allData;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return list.toArray(new String[0]);
    }


    @Override
    public String execute() {
        String[] result = getAllTable();
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("----------------\n");
        stringBuilder.append("| 数据表 |   id  |\n");
        stringBuilder.append("----------------\n");
        for (String str : result) {
            stringBuilder.append("| " +str + " |");
            stringBuilder.append("\n");
        }
        stringBuilder.append("--------\n");
        stringBuilder.append("总数:"+ result.length +"\n");
        return stringBuilder.toString();
    }


    public List<TableMetadata> getResultList() {
        return resultList;
    }
}

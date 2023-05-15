package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class ShowDatabasesCommand extends AbstractCommand {


    @Override
    public String[] exec() {
        List<String> list = new ArrayList<>();
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
            List<DatabaseMetadata> allData = metadataStore.getAllData();

            for (int i = 0; i < allData.size(); i++) {
                list.add(allData.get(i).getName());
            }
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
        String[] result = exec();
        StringBuilder stringBuilder = new StringBuilder();
        for (String str : result) {
            stringBuilder.append(str);
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

}

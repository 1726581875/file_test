package test.command;

import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateTableCommand;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMetadata;
import com.moyu.xmz.terminal.util.PrintResultUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class CreateTableCommandTest {


    public static void main(String[] args) {
        Integer databaseId = 0;
        testCreateTableCommand(databaseId);
        printTableInfo(0);
    }


    private static void testCreateTableCommand(Integer databaseId) {
        CreateTableCommand command = new CreateTableCommand();
        command.setDatabaseId(0);
        command.setTableName("haofan");

        List<Column> columnDtoList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Column columnDto = new Column("hf_" + i, ColumnTypeConstant.VARCHAR, i, 64);
            columnDtoList.add(columnDto);
        }
        command.setColumnList(columnDtoList);
        QueryResult queryResult = command.execCommand();
        PrintResultUtil.printResult(queryResult);
    }



    private static void printTableInfo(Integer databaseId) {
        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(0);
            TableMetaFileAccessor finalMetadataStore = metadataStore;
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
                System.out.println("==== table ==== ");
                System.out.println(tableMetadata);
                List<ColumnMetadata> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
                columnList.forEach(System.out::println);
                System.out.println("==== table ==== ");
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }



}

package test.command;

import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateTableCmd;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMeta;
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
        CreateTableCmd command = new CreateTableCmd();
        command.setDatabaseId(0);
        command.setTableName("haofan");

        List<Column> columnDtoList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Column columnDto = new Column("hf_" + i, DbTypeConstant.VARCHAR, i, 64);
            columnDtoList.add(columnDto);
        }
        command.setColumnList(columnDtoList);
        QueryResult queryResult = command.execCommand();
        PrintResultUtil.printResult(queryResult);
    }



    private static void printTableInfo(Integer databaseId) {
        TableMetaAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaAccessor(0);
            TableMetaAccessor finalMetadataStore = metadataStore;
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
                System.out.println("==== table ==== ");
                System.out.println(tableMetadata);
                List<ColumnMeta> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
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

package test.command;

import com.moyu.test.command.ddl.CreateTableCommand;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;

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
            Column columnDto = new Column("hf_" + i, DbColumnTypeConstant.VARCHAR, i, 64);
            columnDtoList.add(columnDto);
        }
        command.setColumnDtoList(columnDtoList);

        String execute = command.execute();
        System.out.println(execute);
    }



    private static void printTableInfo(Integer databaseId) {
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(0);
            TableMetadataStore finalMetadataStore = metadataStore;
            metadataStore.getAllTable().forEach(tableMetadata -> {
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

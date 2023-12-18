package test.export;

import com.moyu.test.command.Command;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.SqlParser;
import com.moyu.test.net.model.terminal.ColumnMetaDto;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.model.terminal.RowDto;
import com.moyu.test.session.ConnectSession;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/12/14
 */
public class ExportTableTest {

    public static void main(String[] args) throws Exception {
        ConnectSession connectSession = new ConnectSession("xmz", 1);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand("select * from t_event_record");
        QueryResult queryResult = command.execCommand();




        File file = new File("D:\\logs\\t_event_record.csv");
        if(!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream dos = new FileOutputStream(file);

        QueryResultDto queryResultDto = QueryResultDto.valueOf(queryResult);
        ColumnMetaDto[] columns = queryResultDto.getColumns();
        RowDto[] rows = queryResultDto.getRows();

        StringBuilder stringBuilder = new StringBuilder("");
        for (int i = 0; i < columns.length; i++) {
            stringBuilder.append(columns[i].getColumnName());
            if(i != columns.length - 1) {
                stringBuilder.append(",");
            } else {
                stringBuilder.append("\n");
            }
        }


        for (int i = 0; i < rows.length; i++) {
            Object[] rowValues = rows[i].getColumnValues();
            for (int j = 0; j < rowValues.length; j++) {
                String valueStr = valueToString(rowValues[j]);
                stringBuilder.append(valueStr);
                if(j != columns.length - 1) {
                    stringBuilder.append(",");
                } else {
                    stringBuilder.append("\n");
                }
            }
        }


        dos.write(stringBuilder.toString().getBytes());
        dos.flush();
        dos.close();
        System.out.println(stringBuilder);
    }


    private static String valueToString(Object value) {
        if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.format((Date) value);
        } else {
            return String.valueOf(value);
        }
    }

}

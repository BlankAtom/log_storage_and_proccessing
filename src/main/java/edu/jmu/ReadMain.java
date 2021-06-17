package edu.jmu;

import org.apache.hadoop.hbase.client.Put;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/16/20:00
 */
public class ReadMain {
    public static void main(String[] args) {
        if( args.length <= 1 ) {
            System.out.println("You need more parameters to tell me where is the file!");
//            return;
        }
//        File file = new File(args[1]);

        long time = new Date().getTime();
        File file = new File("D:\\ImDeveloper\\code\\java\\rocael_spark_demo\\spark-demo\\src\\main\\resources\\test.txt");
        BufferedReader bufferedReader = null;
        int line = 1;
        String tempStr;
        ReadMain readMain = new ReadMain();
        List<Put> puts = new ArrayList<Put>();
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
            while( (tempStr = bufferedReader.readLine()) != null ){
                String[] s = tempStr.split("\\s+");

                // start_time user_id, search_word, url_reward, user_click_no, user_click_url
                if(s.length != 6 ) {
                    System.out.println("line " + line + " is error data. and length is " + s.length);
                }
                else {
//                    System.out.println(Arrays.toString(s));
                    puts.addAll(readMain.instancePut(Integer.toString(line), s[0], s[1], s[2], s[3], s[4], s[5]));
                }
                line++;

                if( line % 3000 == 0){
                    HBaseDemoMain.insertDatas("log_records", puts);
                    puts = new ArrayList<>();

                    System.out.println("3000 datas is inserting.");
                }
            }
            HBaseDemoMain.insertDatas("log_records", puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Put> instancePut(String rowkey, String start_time, String user_id, String search_word,
                           String url_reward, String user_click_no, String user_click_url){
        List<Put> puts = new ArrayList<>();
        Put put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "start_time".getBytes(StandardCharsets.UTF_8),
                start_time.getBytes(StandardCharsets.UTF_8));
        puts.add(put);
        put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "user_id".getBytes(StandardCharsets.UTF_8),
                user_id.getBytes(StandardCharsets.UTF_8));
        puts.add(put);
        put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "search_word".getBytes(StandardCharsets.UTF_8),
                search_word.getBytes(StandardCharsets.UTF_8));
        puts.add(put);
        put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "url_reward".getBytes(StandardCharsets.UTF_8),
                url_reward.getBytes(StandardCharsets.UTF_8));
        puts.add(put);
        put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "user_click_no".getBytes(StandardCharsets.UTF_8),
                user_click_no.getBytes(StandardCharsets.UTF_8));
        puts.add(put);
        put = new Put(rowkey.getBytes(StandardCharsets.UTF_8));
        put.addColumn("info".getBytes(StandardCharsets.UTF_8),
                "user_click_url".getBytes(StandardCharsets.UTF_8),
                user_click_url.getBytes(StandardCharsets.UTF_8));
        puts.add(put);

        return puts;
    }
}

package edu.jmu.hbase;

import edu.jmu.rdd.SparkDB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.yarn.api.records.URL;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/20/21:03
 */
public class DataFilter {

    public static void main(String[] args) throws IOException, InterruptedException {
        if( args.length == 0) {
            System.out.println(new String("你需要更多的参数!".getBytes(), 0, "你需要更多的参数!".length(), "UTF-8"));
            System.out.println("eg. hadoop jar xxx.jar /input/Sogou.txt");
            System.exit(0);
        }
        createTable(args);
    }

    public static void createTable(String[] args) throws IOException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fsSource = FileSystem.get(URI.create("hdfs://localhost:9000" ), conf, "root");
        FSDataInputStream out = fsSource.open(new Path(args[0]));
        long time = new Date().getTime();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(out));
        int line = 1;
        String tempStr;
        DataFilter readMain = new DataFilter();
        List<Put> puts = new ArrayList<>();
        try {
            while( (tempStr = bufferedReader.readLine()) != null ){
                List<String> s = SparkDB.splitRecord(tempStr);

                // start_time user_id, search_word, url_reward, user_click_no, user_click_url
                if(s.size() != 6 ) {
                    System.out.println("line " + line + " is error data. and length is " + s.size());
                }
                else {
//                    System.out.println(Arrays.toString(s));
                    puts.addAll(readMain.instancePut(Integer.toString(line), s.get(0), s.get(1), s.get(2), s.get(3), s.get(4), s.get(5)));
                }
                line++;

                if( line % 4000 == 0){
                    HBaseDemoMain.insertDatas("test_records", puts);
                    puts = new ArrayList<>();
                    System.out.println("4000 datas is inserting.");
                }
            }
            HBaseDemoMain.insertDatas("test_records", puts);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(new Date().getTime() - time);
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

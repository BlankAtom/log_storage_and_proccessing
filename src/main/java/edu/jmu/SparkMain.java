package edu.jmu;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/18/13:55
 */
public class SparkMain {

    // 条件查询
    // 查询条件是 某字段  某属性
    // 支持联合搜索
    // 1、时间段
    // 2、输入IDs， 获得所有匹配内容
    // 3、搜索含有关键字的记录
    // 4、根据域名关键字获取记录
//
//    public static void main(String[] args) throws MalformedURLException {
//        String s = "D:\\ImDeveloper\\code\\java\\LogProccessing\\src\\main\\resources\\text.txt";
//        SparkConf sparkConf = new SparkConf().setAppName("Test");//.setMaster("local");
//        //JavaSparkContext sc = new JavaSparkContext("local", "SparkMain",
//          //      "file:///root/", new String[]{"LogProccessing-1.0.jar"});
////        SparkContext sparkContext = new SparkContext();:
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        JavaRDD<String> logData = sc.textFile("hdfs://h01:9000/input/input");//.cache();
//        long numAs = logData.filter(new Function<String, Boolean>() {
//            @Override
//            public Boolean call(String s) throws Exception {
//                return s.contains("00:00:00");
//            }
//        }).count();
//        System.out.println("Lines with numAs: "+numAs);
////        String s1 = "https://blog.baidu.com/123/123";
////
////        String[] split = s1.split("/");
////        String[] split1 = split[0].split(".");
////        URL url = new URL(s1);
////        System.out.println(url.getHost());
////
////        String url = "http://ask.csdn.net/questions/237143";
////        Pattern p = Pattern.compile("(?<=http://|\\.)[^.]*?\\.(com|cn|net|org|biz|info|cc|tv|xyz)",Pattern.CASE_INSENSITIVE);
////
////        Matcher matcher = p.matcher(url);
////        matcher.find();
////        System.out.println(matcher.group()); // csdn.net
//
//
//    }
}

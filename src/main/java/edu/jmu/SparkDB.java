package edu.jmu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/19/10:50
 */
public class SparkDB {
    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName"));
        JavaRDD<String> logData = sc.textFile(inputFile).cache();
        List<String> result = new ArrayList<String>();
        long numAs = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                List<String> list = SparkDB.splitRecord(s);

                if (SparkDB.enableTimeCompare ) {

                }
                return s.contains("00:00:00");
            }
        }).count();
    }

    public static boolean enableTimeCompare = false;
    public static boolean enableIDCompare = false;
    public static boolean enableWordCompare = false;
    public static boolean enableUrlCompare = false;

    public static String sTime_start = "";
    public static String sTime_stop = "";
    public static ArrayList<String> sWord = new ArrayList<>();
    public static ArrayList<String> sUrl = new ArrayList<>();
    public static ArrayList<String> sID = new ArrayList<>();

    public static List<String> splitRecord(String s ){
        String[] s1 = s.split("\\[");
        String[] s2 = s1[1].split("]");
        String[] split_left = s1[0].split("\\s+");
        String[] split_right = s2[1].split("\\s+");
        ArrayList<String> res = new ArrayList<>(Arrays.asList(split_left));
        res.add(s2[0]);
        res.addAll(Arrays.asList(split_right));

        return res;
    }
    public void writeFile(String file) {

    }

    /**
     * 判断时间是否在输入时间的区间之内
     * 判断标准是左闭右闭
     * @param nowTime 需要判断的时间，字符串
     * @param startTime 起始时间，字符串
     * @param stopTime 终止时间，字符串
     * @return true在区间内，false不在
     * @throws ParseException 可能会在解析字符串的时候产生异常
     */
    public static boolean isInTimeLine(String nowTime,
                                String startTime,
                                String stopTime) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
        Date start = format.parse(startTime);
        Date stop = format.parse(stopTime);
        Date time = format.parse(nowTime);
        return time.compareTo(start) >= 0 && time.compareTo(stop) <= 0;
    }
    public static boolean isInTimeLine(String nowTime) throws ParseException {
        return isInTimeLine(nowTime, sTime_start, sTime_stop);
    }

    /**
     * Compare with all input users' ids
     *
     * @param id wait for comparing
     * @param ids input ids
     * @return compare result
     */
    public static boolean isSearchID(String id,
                              List<String> ids){
        for (String item : ids) {
            if (id.equals(item)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isSearchID(String id) {
        return isSearchID(id, sID);
    }

    /**
     *
     * @param word
     * @param words
     * @return
     */
    public static boolean isSearchWord(String word,
                                List<String> words){
        for (String item : words) {
            if (word.equals(item)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isSearchWord(String word){
        return isSearchWord(word, sWord);
    }

    /**
     * compare with url name
     * @param urlWord need compare
     * @param urlWords input url
     * @return compare result
     */
    public static boolean isSearchUrlName(String urlWord,
                                   List<String> urlWords){
        for (String item : urlWords) {
            if (urlWord.equals(item)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isSearchUrlName(String urlWord){
        return isSearchUrlName(urlWord, sUrl);
    }

}

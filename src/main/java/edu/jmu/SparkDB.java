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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/19/10:50
 */
public class SparkDB {

    public static boolean parseInputSearch(String s)  {
        String[] ss = s.split("\\+");
        List<String> inputs = new ArrayList<>();
        for (String value : ss) {
            inputs.add(value.trim());
        }
        inputs.forEach((item) -> {
            isTimeInput(item);
            isIDInput(item);
            isWordInput(item);
            isUrlInput(item);
        });

        return true;

    }

    public static void whatSearch(String search) {


    }

    public static boolean isUrlInput(String s) {
        String[] split = s.split("\\|");
        for (String s1 : split) {
            if( isUrl(s1.trim())){
                enableUrlCompare = true;
                SparkDB.sUrl.add(s1.trim());
            }
        }
        return true;
    }

    public static boolean isUrl(String s) {
        Pattern p = Pattern.compile("(?<=http://|\\.)[^.]*?\\.(com|cn|net|org|biz|info|cc|tv|xyz)",Pattern.CASE_INSENSITIVE);
        return  p.matcher(s).matches();
    }

    public static boolean isWordInput(String s) {
        String[] split = s.split("\\|");
        for (String value : split) {
            if (isWord(value.trim())) {
                enableWordCompare = true;
                SparkDB.sWord.add(value.trim());
            }
        }
        return true;
    }

    public static boolean isWord(String s) {
        return !isTime(s.trim()) && !isId(s.trim()) && !isUrl(s.trim());
    }

    public static boolean isIDInput(String s) {
        String[] split = s.split("\\|");
        for (String value : split) {
            if (isId(value.trim())) {
                SparkDB.enableIDCompare = true;
                SparkDB.sID.add(value.trim());
            }
        }
        return enableIDCompare;
    }
    public static boolean isId(String s) {
        Pattern pattern = Pattern.compile("[0-9]+");
        return  pattern.matcher(s).matches();
    }
    public static boolean isTimeInput(String s) {
        String[] split = s.split("\\|");
        if(split.length == 2) {
            if(  isTime(split[0].trim()) && isTime(split[1].trim()) ){
                enableTimeCompare = true;
                sTime.add(split[0].trim());
                sTime.add(split[1].trim());
                return true;
            }
        }
        System.out.println("Input error time: " + s);
        return false;
    }
    public static boolean isTime(String s) {
        Pattern pattern = Pattern.compile("[0-2][0-9]:[0-6][0-9]:[0-6][0-9]");
        return  pattern.matcher(s).matches();
    }

    public static boolean isUserID(String s) {
        Pattern pattern = Pattern.compile("\\d+", Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(s);
        return matcher.matches();
    }

    public static void main(String[] args) {
        String inputFile = "hdfs://h01:9000/input/input" ;//args[0];
        String outputFile = "hdfs://h01:9000/output/outfile"; //args[1];

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName"));
        JavaRDD<String> logData = sc.textFile(inputFile).cache();
        List<String> result = new ArrayList<String>();
        parseInputSearch("00:00:00 | 00:01:00 + 2982199073774412 + 360 + it.com");
        JavaRDD<String> res = logData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                List<String> list = SparkDB.splitRecord(s);
                boolean compare = false;
                if (SparkDB.enableTimeCompare ) {
                    compare = isInTimeLine(list.get(0));
                }
                if( SparkDB.enableIDCompare) {
                    compare = isSearchID(list.get(1));
                }
                if (SparkDB.enableWordCompare){
                    compare = isSearchWord(list.get(2));
                }
                if( SparkDB.enableUrlCompare) {
                    compare = isSearchUrlName(list.get(5));
                }
                return compare;
            }
        });
        res.saveAsTextFile(outputFile);
    }

    public static boolean enableTimeCompare = false;
    public static boolean enableIDCompare = false;
    public static boolean enableWordCompare = false;
    public static boolean enableUrlCompare = false;

    public static String sTime_start = "";
    public static String sTime_stop = "";
    public static ArrayList<String> sTime = new ArrayList<>();
    public static ArrayList<String> sWord = new ArrayList<>();
    public static ArrayList<String> sUrl = new ArrayList<>();
    public static ArrayList<String> sID = new ArrayList<>();

    public static List<String> splitRecord(String s ){
        String[] s1 = s.split("\\[");
        String[] s2 = s1[1].split("\\]");
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
        return enableTimeCompare && isInTimeLine(nowTime, sTime.get(0), sTime.get(1));
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
        return enableIDCompare && isSearchID(id, sID);
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
        return enableWordCompare && isSearchWord(word, sWord);
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
        return enableUrlCompare && isSearchUrlName(urlWord, sUrl);
    }

}

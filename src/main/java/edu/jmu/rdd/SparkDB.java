package edu.jmu.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/19/10:50
 */
public class SparkDB {

    public static void parseInputSearch(String s)  {
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

    }

    public static void isUrlInput(String s) {
        String[] split = s.split("\\|");
        for (String s1 : split) {
            if( isUrl(s1.trim())){
                enableUrlCompare = true;
                SparkDB.sUrl.add(s1.trim());
            }
        }
    }

    public static boolean isUrl(String s) {
        Pattern p = Pattern.compile("[^.]*?\\.(com|cn|net|org|biz|info|cc|tv|xyz|hk)",Pattern.CASE_INSENSITIVE);
        Pattern pp = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
        return  p.matcher(s).matches() || pp.matcher(s).matches();
    }
    public static String getUrlName(String s) {
        Pattern p = Pattern.compile("[^.]*?\\.(com|cn|net|org|biz|info|cc|tv|xyz|hk)",Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(s);
        Pattern pp = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");
        boolean b = matcher.find();
        if(b)
            return  matcher.group();

        matcher = pp.matcher(s);
        if (matcher.find()) {
            return matcher.group();
        }

        return null;
    }
    public static void isWordInput(String s) {
        String[] split = s.split("\\|");
        for (String value : split) {
            if (isWord(value.trim())) {
                enableWordCompare = true;
                SparkDB.sWord.add(value.trim());
            }
        }
    }

    public static boolean isWord(String s) {
        return !isTime(s.trim()) && !isId(s.trim()) && !isUrl(s.trim());
    }

    public static void isIDInput(String s) {
        String[] split = s.split("\\|");
        for (String value : split) {
            if (isId(value.trim())) {
                SparkDB.enableIDCompare = true;
                SparkDB.sID.add(value.trim());
            }
        }
    }
    public static boolean isId(String s) {
        Pattern pattern = Pattern.compile("^[0-9]{15,20}$");
        return  pattern.matcher(s).matches();
    }
    public static void isTimeInput(String s) {
        String[] split = s.split("\\|");
        if(split.length == 2) {
            if(  isTime(split[0].trim()) && isTime(split[1].trim()) ){
                enableTimeCompare = true;
                sTime.add(split[0].trim());
                sTime.add(split[1].trim());
                return;
            }
        }
        System.out.println("Input error time: " + s);
    }
    public static boolean isTime(String s) {
        Pattern pattern = Pattern.compile("[0-2][0-9]:[0-6][0-9]:[0-6][0-9]");
        return  pattern.matcher(s).matches();
    }

    public static int findRightMiddle(String s, int  startIndex){
        StringBuilder buffer = new StringBuilder(s);
        for (int i = startIndex; i < s.length(); i++) {
            if( buffer.charAt(i)=='\t'){
                if( buffer.charAt(i+1) >= '0' && buffer.charAt(i+1) <= '9'){
                    return i-1;
                }
            }

        }

        return s.length()-1;
    }

    public static void main(String[] args) {
        String inputFile = "hdfs://localhost:9000/input/SogouQ.txt" ;//args[0];
        String outputFile = "hdfs://localhost:9000/output"; //args[1];

        if(args.length==0 || "--help".equals(args[0])){
            System.out.println("\t\"search\":  条件查询，");
            System.out.println("\t\teg. 00:00:00 | 00:01:00 + 2982199073774412 + 360 + it.com");
            System.out.println("\t\"flow\": 流量统计. ");
            System.out.println("\t\teg. 00:00:00 00:01:00");
            System.out.println("\t\"user\": 使用频率统计. (无参数)");
            System.out.println("\t\"action\": 访问行为统计. (无参数)");

            System.exit(0);
        }
        if( "action".equals(args[0])) {
            searchActionMath(inputFile, outputFile+"/action");
        }
        if( "user".equals(args[0])) {
            searchUserTimes(inputFile, outputFile+"/user");
        }
        if( "flow".equals(args[0])) {
            if(args.length < 3){
                System.out.println("Too least parameters!");
                System.exit(0);
            }
            timeStreamCompute(inputFile, outputFile+"/flow", args[1], args[2]);
        }
        if( "search".equals(args[0]) ){
            if(args.length < 2){
                System.out.println("Please enter search word");
                System.exit(0);
            }
            StringBuilder str = new StringBuilder();
            for (int i = 1; i < args.length; i++) {
                str.append(" ").append(args[i]);
            }
            System.out.println("enter: " + str);
            rddStartSearch(inputFile, outputFile+"/search", new String(str) );
        }

    }

    public static void searchActionMath(String input, String output) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName").setMaster("local"));
        JavaRDD<String> logData = sc.textFile(input);

        JavaPairRDD<String, Integer> pairRDD = logData.mapToPair((PairFunction<String, String, Integer>) s -> {
            List<String> list = SparkDB.splitRecord(s);
            return new Tuple2<>(list.get(3), 1);
        });
        JavaPairRDD<String, Integer> rdd = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        rdd.foreach(System.out::println);
        rdd.saveAsTextFile(output);
    }

    public static void searchUserTimes(String input, String output) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName").setMaster("local"));
        JavaRDD<String> logData = sc.textFile(input);
        System.out.println(input);
        System.out.println("to: " + output);
        JavaPairRDD<String, Integer> pairRDD = logData.mapToPair((PairFunction<String, String, Integer>) s -> {
            List<String> list = SparkDB.splitRecord(s);
            return new Tuple2<>(list.get(1), 1);
        });
        JavaPairRDD<String, Integer> rdd = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
//        rdd.foreach(System.out::println);
        rdd.saveAsTextFile(output);
    }
    public static void timeStreamCompute(String input, String output, String time_start, String time_stop) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName").setMaster("local"));

        JavaRDD<String> logData = sc.textFile(input);
        JavaRDD<String> res = logData.filter((Function<String, Boolean>) s -> {
            List<String> list = SparkDB.splitRecord(s);
            return isInTimeLine(list.get(0), time_start, time_stop);
        });

        JavaPairRDD<String, Integer> pairRDD = res.mapToPair((PairFunction<String, String, Integer>) s -> {
            List<String> list = SparkDB.splitRecord(s);
            return new Tuple2<>(list.get(2), 1);
        });
        JavaPairRDD<String, Integer> urlRdd = res.mapToPair((PairFunction<String, String, Integer>) s -> {
            List<String> list = SparkDB.splitRecord(s);
            String urlName = SparkDB.getUrlName(list.get(5));
            if( urlName == null ){
                urlName = list.get(5);
            }
            return new Tuple2<>(urlName, 1);
        });
        JavaPairRDD<String, Integer> wordPairRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        JavaPairRDD<String, Integer> urlRDD = urlRdd.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
        wordPairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) stringIntegerTuple2 -> {
            String s = stringIntegerTuple2._1 + "  " + stringIntegerTuple2._2;
            System.out.println(s);
        });
        urlRDD.foreach((VoidFunction<Tuple2<String, Integer>>) stringIntegerTuple2 -> {
            String s = stringIntegerTuple2._1 + "  " + stringIntegerTuple2._2;
            System.out.println(s);
        });
        wordPairRDD.saveAsTextFile(output+"/timeWord");
        urlRDD.saveAsTextFile(output+"/timeUrl");
    }

    public static void rddStartSearch(String input, String output, String search) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkName").setMaster("local"));
        JavaRDD<String> logData = sc.textFile(input).cache();
        parseInputSearch(search);
        System.out.println(sWord);
        System.out.println(sUrl);
        System.out.println(sTime);
        System.out.println(sID);
        JavaRDD<String> res = logData.filter((Function<String, Boolean>) s -> {
            List<String> list = SparkDB.splitRecord(s);
//            System.out.println(list);
            boolean compare = true;
            if (SparkDB.enableTimeCompare ) {
                compare = isInTimeLine(list.get(0));
            }
            if( SparkDB.enableIDCompare) {
                compare = compare && isSearchID(list.get(1));
            }
            if (SparkDB.enableWordCompare){
                compare = compare && isSearchWord(list.get(2));
            }
            if( SparkDB.enableUrlCompare) {
                compare =  compare && isSearchUrlName(list.get(5));
            }
            if (compare) {
                System.out.println(s);
            }
            return compare;
        });
//        res.count();
        res.saveAsTextFile(output);
    }

    public static boolean enableTimeCompare = false;
    public static boolean enableIDCompare = false;
    public static boolean enableWordCompare = false;
    public static boolean enableUrlCompare = false;

    public static ArrayList<String> sTime = new ArrayList<>();
    public static ArrayList<String> sWord = new ArrayList<>();
    public static ArrayList<String> sUrl = new ArrayList<>();
    public static ArrayList<String> sID = new ArrayList<>();

    public static List<String> splitRecord(String s ){
        int leftIndex = s.indexOf("[");
        int rightIndex = findRightMiddle(s, leftIndex);
        String[] split_left = s.substring(0, leftIndex).trim().split("\\s+");
        String[] split_right = s.substring(rightIndex+1).trim().split("\\s+");
        List<String> strings = new ArrayList<>(Arrays.asList(split_left));
        String trim = s.substring(leftIndex + 1, rightIndex).trim();
        strings.add(trim);
        strings.addAll(Arrays.asList(split_right));
        if (strings.size() < 6) {
            System.out.println("parse failed: " + s);
            strings.add("");
            strings.add("");strings.add("");
        }
//        "12:58:08\t8955143566377745\t[[uusee网络电视]\t2 1\twww.cnuusee.cn/"

        return strings;
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
     * @param word -
     * @param words -
     * @return -
     */
    public static boolean isSearchWord(String word,
                                List<String> words){
        for (String item : words) {
            if (word.contains(item)) {
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
            if (urlWord.contains(item)) {
                return true;
            }
        }
        return false;
    }
    public static boolean isSearchUrlName(String urlWord){
        return enableUrlCompare && isSearchUrlName(urlWord, sUrl);
    }

}

package edu.jmu.hbase;

import java.io.*;



/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/16/20:00
 */
public class ReadMain {
    public static void main(String[] args) throws IOException {

        /*String keywords = "#+8561366108033201+汶川地震+www.big38";
        String[] word = keywords.split("\\+");
        for (int i = 0;i<word.length;i++)
            System.out.println(word[i]);

         */
        boolean flag = true;
        while(flag) {
            System.out.println("请输入你想要的查询方式");
            System.out.println("1.根据开始时间和结束时间查询");
            System.out.println("2.根据用户ID查询");
            System.out.println("3.根据关键词查询");
            System.out.println("4.根据url查询");
            System.out.println("5.联合查询");
            System.out.println("0.退出");

            BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
            String choose = buffer.readLine();
            switch(choose){
                case "1":
                    System.out.println("请按照 开始时间|结束时间 的方式进行输入");
                    BufferedReader b1 = new BufferedReader(new InputStreamReader(System.in));
                    String time = b1.readLine();
                    HBaseDemoMain.searchTime("test_records",time);
                    break;
                case "2":
                    System.out.println("请按照 用户ID|用户ID|... 的方式进行输入");
                    BufferedReader b2 = new BufferedReader(new InputStreamReader(System.in));
                    String ids = b2.readLine();
                    HBaseDemoMain.searchID("test_records",ids);
                    break;
                case "3":
                    System.out.println("请按照 关键字|关键字|... 的方式进行输入");
                    BufferedReader b3 = new BufferedReader(new InputStreamReader(System.in));
                    String words = b3.readLine();
                    HBaseDemoMain.searchKeyword("test_records",words);
                    break;
                case "4":
                    System.out.println("请按照 url|url|... 的方式进行输入");
                    BufferedReader b4 = new BufferedReader(new InputStreamReader(System.in));
                    String urls = b4.readLine();
                    HBaseDemoMain.searchUrl("test_records",urls);
                    break;
                case "5":
                    System.out.println("请按照 开始时间|结束时间+用户ID+关键字+url 的方式进行输入");
                    System.out.println("不需要的条件用#代替");
                    BufferedReader b5 = new BufferedReader(new InputStreamReader(System.in));
                    String word = b5.readLine();
                    HBaseDemoMain.searchALL("test_records",word);
                    break;
                case "0":
                    flag = false;
                    break;
            }
        }
    }
}

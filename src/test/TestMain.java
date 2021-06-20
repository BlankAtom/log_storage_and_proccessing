import edu.jmu.rdd.SparkDB;
import org.junit.Test;

import java.util.Arrays;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/19/0:09
 */
public class TestMain{

    @Test
    public void fun() {
        String s = "123[321]321";
        String[] split = s.split("\\[");
        String[] split1 = split[1].split("\\]");
        System.out.println(Arrays.toString(split1));
    }

    @Test
    public void fun3() {
        System.out.println(SparkDB.splitRecord("00:18:56\t620388917049539\t[長春旅遊]\t1 1\twww.ccta.gov.cn/"));
    }

    @Test
    public void fun4() {
        System.out.println(SparkDB.getUrlName("https://127.0.0.1/bbs/index"));
    }

    @Test
    public void regex() {
//        System.out.println(SparkDB.isTime("01:02:03 |   00:99:05"));

//        System.out.println(SparkDB.parseInputSearch("01:02:03 |   00:99:05 +  123 | 312 + hei + baidu.com"));
    }

}

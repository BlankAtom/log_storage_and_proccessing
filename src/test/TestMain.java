import edu.jmu.SparkDB;
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
    public void regex() {
//        System.out.println(SparkDB.isTime("01:02:03 |   00:99:05"));

//        System.out.println(SparkDB.parseInputSearch("01:02:03 |   00:99:05 +  123 | 312 + hei + baidu.com"));
    }

}

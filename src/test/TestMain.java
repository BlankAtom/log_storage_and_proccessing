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
        String s = "123[321]";
        String[] split = s.split("\\[");
        System.out.println(Arrays.toString(split));
    }
}

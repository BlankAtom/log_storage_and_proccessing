package edu.jmu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/17/19:38
 */
public class Hbase2Hdfs implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}

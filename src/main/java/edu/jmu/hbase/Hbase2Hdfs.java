package edu.jmu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.hbase.mapreduce.*;


/**
 * <p></p>
 *
 * @author github/blackswords
 * @date 2021/06/17/19:38
 */
public class Hbase2Hdfs implements Tool {

    public Configuration configuration;
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("hbase.zookeeper.quorum","h01");  //hbase 服务地址
        configuration.set("fs.defaultFS", "hdfs://h01:9000");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        this.configuration = configuration;
    }

    public static class HBaseMapper extends Mapper<Text, Text, Text, Text> {

    }
    public static class HBaseReduce extends Reducer<Text, Text, Text, Text> {

    }
    @Override
    public Configuration getConf() {
        return null;
    }
}

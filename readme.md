# 大数据课程设计：用户查询日志的存储与处理

## 一、任务要求
1. 数据清洗：载入文本中的日志数据，每行作为一条记录，分为访问时间，用户ID，查询词，返回结果排名，顺序号，URL这六个字段（列），存入HBASE。

2. HBase条件查询、联合查询：

      -  时间段：输入信息为开始时间和结束时间，用`|`字符隔开
         
      -  用户ID：输入信息为一个或多个用户ID，用`|`字符隔开
         
      -  搜索词关键字：输入信息为一个或多个关键字，用`|`字符隔开
         
      -  url访问记录：输入信息为一个或多个关键字，用`|`字符隔开
         
      -  以上四个条件的联合查询，使用`+`号分割条件

3. 使用Hadoop的MapReduce或Spark的RDD，实现以下功能：
      
      - 基于大数据计算技术的条件查询：使用`MapReduce`框架或`RDD`算子，实现类似于HBase的六个字段条件搜索。
        
      -  时段流量统计：以`hh:mm:ss`格式输入起始时间和结束时间，统计这段时间之内的总搜索次数、各个查询词搜索次数，各个网站的访问量。
         
      -  用户使用频率统计：统计每个用户一天内的搜索次数
         
      -  访问行为统计：根据该页面在搜索结果中的排名（第 4 字段），统计不同排名的结果被访问的情况。


## 二、实现内容

### 1）数据清洗

#### a. 获取文件流

```java

class DataFilter {
    // ...
    public static void main(String[] args) {
        //  ...
        FileSystem fsSource = FileSystem.get(
                URI.create("hdfs://localhost:9000"), 
                conf, 
                "root"
        );
        FSDataInputStream out = fsSource.open(new Path(args[0]));
        long time = new Date().getTime();
        int line = 1;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(out));
        // ...
    }
}
```

使用`FileSystem.get()` 方法获取到一个可用的`FileSystem`
这里用到的hdfs链接和`"root"`，都可以另外作为变量来使用
 
*root字符串的意思是用户名，是具有操作hdfs权限的用户的名字（Linux）*




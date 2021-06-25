package edu.jmu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
/**
 * @Author: 罗庆宏
 * @Date: 2021/04/12/22:29
 * @Description:
 */
public class HBaseDemoMain {
//    public static Configuration configuration = null;
    public static Connection connection = null;
    public static Admin admin = null;

    static  {
        try {
            //1. Get configuration info.
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","159.75.90.116");  //hbase 服务地址
            configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号

            //2. Create link object.
            connection = ConnectionFactory.createConnection(configuration);

            //3. Create Admin Object
            admin = connection.getAdmin();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    //2. 判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        return admin != null && admin.tableExists(TableName.valueOf(tableName));
    }
    //3. 关闭
    public static void close(){
        try {

            if (admin!=null){
                admin.close();
            }
            if (connection!=null){
                connection.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 4. 建表
    public static void createTable(String tableName, String... cfs) throws IOException {
        //1. 判断是否存在参数
        if(cfs.length <= 0){
            System.out.println("Please set column family.");
            return ;
        }

        //2. 判断是否表存在
        if(isTableExist(tableName)){
            System.out.println(tableName + " is exist.");
            return ;
        }

        //3. 创建表描述器
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        //4. 循环添加列族信息
        for (String cf : cfs) {
            //5. 创建列族描述器
            ColumnFamilyDescriptor of = ColumnFamilyDescriptorBuilder.of(cf);

            //6. 添加具体的列族信息
            builder.setColumnFamily(of);
        }

        //7. 创建表
        admin.createTable(builder.build());

    }

    //5. 向表插入数据

    /**
     * <p>Hello</p>
     * @param tableName 表名
     * @param rowKey 行键
     * @param cf 列族
     * @param cn 列名
     * @param value 插入的值
     * @throws IOException 抛出异常
     */
    public static void putData(String tableName, String rowKey,
                               String cf, String cn, String value) throws IOException
    {
        //1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2. 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3. 给Put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //4. 插入数据
        table.put(put);

        //5. 关闭table
        table.close();
    }
    //5. 插入数据
    public static void insertData(String tableName, String rowKey,
                                  String colFamily, String col,
                                  String val) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes(StandardCharsets.UTF_8));

        put.addColumn(colFamily.getBytes(StandardCharsets.UTF_8), col.getBytes(StandardCharsets.UTF_8), val.getBytes(StandardCharsets.UTF_8));
        table.put(put);
        table.close();
    }
    // 批量插入
    public static void insertData(String tableName, List<Put> puts) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(puts);
        table.close();
    }

    //6. 获取数据
    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes(StandardCharsets.UTF_8));
        get.addColumn(colFamily.getBytes(StandardCharsets.UTF_8), col.getBytes(StandardCharsets.UTF_8));
        Result result = table.get(get);
        System.out.println(new String(result.getValue(colFamily.getBytes(StandardCharsets.UTF_8), col.getBytes(StandardCharsets.UTF_8)) ));
        table.close();
    }

    /**
     * 修改某行某列的数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param column 列，可以是 "f1:c1" 的形式，也可以不含列限定符
     * @param value 值
     * @throws IOException 排除异常
     */
    public static void modifyData(String tableName, String rowKey,
                                 String column, String value) throws IOException
    {
        Table table= connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes(StandardCharsets.UTF_8));
        String []cols = column.split(":");
        if( cols.length <= 1) {
            // 不含列限定符
            put.addColumn(column.getBytes(StandardCharsets.UTF_8),
                    "".getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));
        }
        else {
            // 含列限定符
            put.addColumn(cols[0].getBytes(StandardCharsets.UTF_8),
                    cols[1].getBytes(StandardCharsets.UTF_8),
                    value.getBytes(StandardCharsets.UTF_8));

        }

        table.put(put);
        table.close();
    }
    //7. 删除表
    public static void dropTable(String tableName) throws IOException {
        if( !isTableExist(tableName)) {
            try {
                //1. 使表下线
                admin.disableTable(TableName.valueOf(tableName));
                //2. 删除表
                admin.deleteTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //8. 创建命名空间
    public static void createNameSpace(String ns) {

        // 1. 创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(ns).build();

        //2. 创建命名空间
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println(ns + " namespace is exist!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 增加记录
    public static void addRecord(String tableName, String row,
                                 String[] column, String[] values) throws IOException
    {
        int length = values.length;
        for ( int i = 0; i< length; i++){
            String[] ss = column[i].split(":");
            if( ss.length > 1)
                insertData(tableName, row, ss[0], ss[1], values[i]);
            else
                insertData(tableName, row, ss[0], "", values[i]);
        }
    }
    //查询一列

    /**
     * <p>查询表中一列的数据，使用列族+列名的形式的，按列族+列名的形式查.
     *  使用列族的形式的，按照列族的形式查</p>
     * @param tableName 表名
     * @param column 列名
     * @throws IOException 抛出IO异常
     */
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        String [] cols = column.split(":");
        if ( cols.length > 1)  {
            scan.addColumn(cols[0].getBytes(StandardCharsets.UTF_8), cols[1].getBytes(StandardCharsets.UTF_8));
        }
        else {
            scan.addFamily(column.getBytes(StandardCharsets.UTF_8));
        }

        System.out.println("RowKey\tValue");
        ResultScanner scanner = table.getScanner(scan);
        for ( Result r : scanner) {
            for ( Cell cell : r.rawCells()) {
                System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + ": \t" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
        table.close();
        scanner.close();

    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Delete delete = new Delete(row.getBytes(StandardCharsets.UTF_8));
        Table table = connection.getTable(TableName.valueOf(tableName));

        table.delete(delete);
        table.close();
    }

    //打印查询的结果
    public static void printResult(Table table, Scan scan){
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                CellScanner cellScanner = result.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    System.out.println(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8) + ":" + new String(CellUtil.cloneValue(cell), StandardCharsets.UTF_8));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //过滤器链基本操作实现
    public static void filterList(String tableName) throws IOException{
        //创建过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //创建两个单值过滤器
        SingleColumnValueFilter singleColumnValueFilter1 =
                new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("user_id"),CompareOperator.EQUAL,Bytes.toBytes("7423866288265172"));
        SingleColumnValueFilter singleColumnValueFilter2 =
                new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("user_id"),CompareOperator.EQUAL,Bytes.toBytes("5"));
        //过滤单值属性列，如果没有这个属性列就不计算它
        singleColumnValueFilter1.setFilterIfMissing(true);
        singleColumnValueFilter2.setFilterIfMissing(true);
        //添加到过滤器链中
        filterList.addFilter(singleColumnValueFilter1);
        filterList.addFilter(singleColumnValueFilter2);
        //创建scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }


    public static SingleColumnValueFilter getSingleFilter(String columnFamily,
                                                          String columnKey,
                                                          CompareOperator compareOperator,
                                                          String compareValue) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(columnFamily.getBytes(), columnKey.getBytes(StandardCharsets.UTF_8),
                compareOperator, compareValue.getBytes(StandardCharsets.UTF_8));
        filter.setFilterIfMissing(true);
        return filter;
    }
    public static SingleColumnValueFilter getSingleFilterBySubString(String columnFamily,
                                                                     String columnKey,
                                                                     CompareOperator compareOperator,
                                                                     String compareValue){
        SingleColumnValueFilter filter = new SingleColumnValueFilter(columnFamily.getBytes(), columnKey.getBytes(StandardCharsets.UTF_8),
                compareOperator, new SubstringComparator(compareValue));
        filter.setFilterIfMissing(true);
        return filter;
    }
    public static SingleColumnValueFilter getSingleFilterBySubString(String columnKey,
                                                                     CompareOperator compareOperator,
                                                                     String compareValue)

    {
        return getSingleFilterBySubString("info", columnKey, compareOperator, compareValue);
    }
    public static SingleColumnValueFilter getSingleFilter(String columnKey,
                                                          CompareOperator compareOperator,
                                                          String compareValue){
        return getSingleFilter("info", columnKey, compareOperator, compareValue);
    }
    public static FilterList getFilterListOfStringArrayBySubString(String columnFamily, String columnKey,
                                                                   CompareOperator operator,
                                                                   FilterList.Operator op, String [] s){
        FilterList filterList = new FilterList(op);
        for (String s1 : s) {
            filterList.addFilter(getSingleFilterBySubString(columnFamily, columnKey, operator, s1));
        }
        return filterList;
    }
    public static FilterList getFilterListOfStringArray(String columnKey, CompareOperator operator,
                                                        String[] s) {
        return getFilterListOfStringArray(columnKey, operator, FilterList.Operator.MUST_PASS_ALL, s);

    }
    public static FilterList getFilterListOfStringArray(String columnKey, CompareOperator operator,
                                                        FilterList.Operator op, String [] s) {
        return getFilterListOfStringArray("info", columnKey, operator, op, s);
    }
    public static FilterList getFilterListOfStringArray(String columnFamily, String columnKey,
                                                        CompareOperator operator,
                                                        FilterList.Operator op, String [] s) {
        FilterList filterList = new FilterList(op);
        for (String s1 : s) {
            filterList.addFilter(getSingleFilter(columnFamily, columnKey, operator, s1));
        }
        return filterList;
    }
    //时间段查询
    //时间段查询
    public static void searchTime(String tableName,String time) throws IOException {

        String[] times = time.split("\\|");
        //创建过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //大于开始时间
        SingleColumnValueFilter singleColumnValueFilter1 =
                new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("start_time"),
                        CompareOperator.GREATER_OR_EQUAL,Bytes.toBytes(times[0]));
        singleColumnValueFilter1.setFilterIfMissing(true);
        filterList.addFilter(singleColumnValueFilter1);
        //小于结束时间
        SingleColumnValueFilter singleColumnValueFilter2 =
                new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("start_time"),
                        CompareOperator.LESS_OR_EQUAL,Bytes.toBytes(times[1]));
        singleColumnValueFilter2.setFilterIfMissing(true);
        filterList.addFilter(singleColumnValueFilter2);

        //创建scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }

    //根据用户ID查询
    public static void searchID(String tableName,String ids) throws IOException {
        //创建一个字符串数组 包含了多个ID
        String[] id = ids.split("\\|");
        //创建过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //通过字符串数组创建多个单值过滤器
        for(int i=0;i<id.length;i++)
        {
            String keyword = id[i];
            SingleColumnValueFilter singleColumnValueFilter =
                    new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("user_id"),CompareOperator.EQUAL,Bytes.toBytes(keyword));
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter);
        }
        //创建scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }

    //根据用户关键词查询
    public static void searchKeyword(String tableName,String words) throws IOException {
        //创建一个字符串数组 包含了多个关键字
        String[] word = words.split("\\|");
        //创建过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //通过字符串数组创建多个单值过滤器
        for(int i=0;i<word.length;i++)
        {
            String keyword = word[i];
            SubstringComparator substringComparator = new SubstringComparator(keyword);
            SingleColumnValueFilter singleColumnValueFilter =
                    new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("search_word"),CompareOperator.EQUAL,substringComparator);
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter);
        }
        //创建scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }

    //根据url查询
    public static void searchUrl(String tableName,String urls) throws IOException {
        //创建一个字符串数组 包含了多个关键字
        String[] url = urls.split("\\|");
        //创建过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //通过字符串数组创建多个单值过滤器
        for(int i=0;i<url.length;i++)
        {
            String keyword = url[i];
            SubstringComparator substringComparator = new SubstringComparator(keyword);
            SingleColumnValueFilter singleColumnValueFilter =
                    new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("user_click_url"),CompareOperator.EQUAL,substringComparator);
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter);
        }
        //创建scan
        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }
    public static void searchALL(String tableName,String keywords) throws IOException{
        //先把输入的字符分割成四个部分
        String[] word = keywords.split("\\+");
        //创建一个过滤器链
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //分割第一部分时间的查询
        if(word[0].equals('#')){
            System.out.println("#");
        }else {
            String[] times = word[0].split("\\|");
            SingleColumnValueFilter singleColumnValueFilter1 =
                    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("start_time"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(times[0]));
            singleColumnValueFilter1.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter1);
            //小于结束时间
            SingleColumnValueFilter singleColumnValueFilter2 =
                    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("start_time"), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(times[1]));
            singleColumnValueFilter2.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter2);
        }
        //第二部分用户ID
        if(word[1].equals('#')){
            System.out.println("#");
        }else {
            String id = word[1];
            SingleColumnValueFilter singleColumnValueFilter3 =
                    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("user_id"), CompareOperator.EQUAL, Bytes.toBytes(id));
            singleColumnValueFilter3.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter3);
        }
        //第三部分关键字查询
        if(word[2].equals('#')){
            System.out.println("#");
        }else {
            String keyword = word[2];
            SubstringComparator substringComparator = new SubstringComparator(keyword);
            SingleColumnValueFilter singleColumnValueFilter4 =
                    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("search_word"), CompareOperator.EQUAL, substringComparator);
            singleColumnValueFilter4.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter4);
        }
        //第四部分url查询
        if(word[3].equals('#')){
            System.out.println("#");
        }else {
            String url = word[3];
            SubstringComparator substringComparator1 = new SubstringComparator(url);
            SingleColumnValueFilter singleColumnValueFilter5 =
                    new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("user_click_url"), CompareOperator.EQUAL, substringComparator1);
            singleColumnValueFilter5.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter5);
        }

        Scan scan = new Scan();
        //设置过滤器
        scan.setFilter(filterList);
        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));
        //打印
        printResult(table,scan);
        //释放资源
        table.close();
    }

}

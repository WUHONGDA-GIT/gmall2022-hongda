package com.hongda.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.bean.TableProcess;
import com.hongda.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * <IN,IN,OUT> 流1数据类型, 流2数据类型, 输出数据类型
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private Connection connection;

    /**
     * 构造器
     * @param stateDescriptor 广播的数据结构描述器.
     */
    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    /**
     * ProcessFunction 都会有的open方法, 在函数运行时, 初始化一些环境对象.
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = new Properties();
        //避免namespace相关问题，添加上配置即可
        properties.put("phoenix.schema.isNamespaceMappingEnabled","true");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER,properties);
    }

    /*

     */

    /**
     *广播流的处理函数, 注意: 返回值为空.
     * @param value 输入: mysqlsource的数据
     *          {"before":null,
     *         "after":{
     *         "source_table":"base_category1",
     *         "sink_table":"dim_base_category1",
     *         "sink_columns":"id,name",
     *         "sink_pk":"id",
     *         "sink_extend":null
     *         }
     *         ,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source"
     *         ,"ts_ms":1664594964927,"snapshot":"false","db":"gmall2022_config","sequence":null,"table":"table_process","server_id":0
     *         ,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1664594964927,"transaction":null}
     * @param ctx
     * @param out : 不输出任何数据
     * @throws Exception
     */

    //TODO: Collector<JSONObject> out 是啥??
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据为JavaBean对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);//解析json, 创建TableProcess对象

        //2.校验表是否存在,如果不存在则建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        //3.将数据写入状态
        String key = tableProcess.getSourceTable();
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        broadcastState.put(key, tableProcess);

    }

    //在Phoenix中校验并建表 create table if not exists db.tn(id varchar primary key,name varchar,....) xxx
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            //处理字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                //获取字段
                String column = columns[i];

                //判断是否为主键字段
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }

                //不是最后一个字段,则添加","
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }

            sql.append(")").append(sinkExtend);

            System.out.println(sql);

            //TODO 预编译SQL
            preparedStatement = connection.prepareStatement(sql.toString());

            //TODO 执行
            preparedStatement.execute();
        /*
        建表异常, 抛出异常, 让任务停止并失败, 方便运维及时排查问题.
         */
        } catch (SQLException e) {
            throw new RuntimeException("建表" + sinkTable + "失败！");
        } finally {
            //TODO 资源释放
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //Value:

    /**
     *
     * @param value kafka source ods_db中的 maxwell record.
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-start","ts":1664606787,"data":{}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":1,"tm_name":"三星","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":2,"tm_name":"苹果","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":3,"tm_name":"华为","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":4,"tm_name":"TCL","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":5,"tm_name":"小米","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":6,"tm_name":"长粒香","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":7,"tm_name":"金沙河","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":8,"tm_name":"索芙特","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":9,"tm_name":"CAREMiLLE","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":10,"tm_name":"欧莱雅","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-insert","ts":1664606787,"data":{"id":11,"tm_name":"香奈儿","logo_url":"/static/default.jpg"}}
     * {"database":"gmall2022","table":"base_trademark","type":"bootstrap-complete","ts":1664606787,"data":{}}

     * @param ctx 只读上下文环境
     * @param out 输出 经过处理的, 带sinkTable字段的 maxwell record.
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table")); // 广播配置数据

        String type = value.getString("type");
        //2. 筛选目标数据: 用flink cdc record 的source table, maxwell record的 type筛选目标数据.
        if (tableProcess != null && ("bootstrap-insert".equals(type) || "insert".equals(type) || "update".equals(type))) {

            //3. 过滤目标数据的列: 用flink cdc record 的 sinkColumns做过滤, 只保留目标列
            filter(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //4. 标记目标数据最后落盘表, 用 flink cdc record 的 sinkTable做标记.
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);
        } else {
            System.out.println("过滤掉：" + value);
        }
    }

    /**
     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"/aaa/bbb"}
     * @param sinkColumns id,tm_name
     *                    <p>
     *                    {"id":"11","tm_name":"atguigu"}
     */
    private void filter(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(split);
//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columnsList.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
        /*
       用maxwell 同步数据记录的列名集合 与 flink cdc 同步数据记录 的列名集合  做交集.
        */
        data.entrySet().removeIf(next -> !columnsList.contains(next.getKey()));
    }
}
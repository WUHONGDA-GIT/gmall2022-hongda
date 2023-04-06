package com.hongda.gmall.realtime.demo.app.transfrom;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;

/**
 * @Date: 2022-10-01 15:13
 * @Description: 针对com.hongda.gmall.realtime.app.func.TableProcessFunction 的 filter函数做实验.
 */
public class JsonAttributeDeleteDemo {
    public static void main(String[] args) {
        //集合1
        JSONObject data = JSON.parseObject("{\"id\":\"11\",\"tm_name\":\"atguigu\",\"logo_url\":\"/aaa/bbb\"}");
        //集合2
        String sinkColumns = "id,tm_name";
        //做交集前.
        System.out.println(data.toJSONString());
        //做交集
        filter(data,sinkColumns);
        //做交集后.
        System.out.println(data.toJSONString());

    }
    public static void filter(JSONObject data, String sinkColumns) {
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
        实验结论: entrySet的Set保存的是json对象的属性地址, 删除操作直接作用于json对象
         */
        data.entrySet().removeIf(next -> !columnsList.contains(next.getKey()));

    }
}

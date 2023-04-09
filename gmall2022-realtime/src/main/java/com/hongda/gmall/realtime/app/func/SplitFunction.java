package com.hongda.gmall.realtime.app.func;

import com.hongda.gmall.realtime.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
/*
这是一个 Java 注解（annotation），用于指定函数的输出类型。
@FunctionHint 允许开发者在 Flink Table API 或 SQL 中定义自定义函数时，为函数提供元数据信息。
其中，@DataTypeHint 用于指定输出参数的数据类型。

表示函数的输出是一个只有一个字段 "word"，类型为字符串（STRING）的行（ROW）。
*/
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        List<String> list;
        try {
            list = KeywordUtil.splitKeyWord(keyword);
            for (String word : list) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }
    }
}

package com.hongda.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.common.GmallConfig;
import com.hongda.gmall.realtime.util.DimUtil;
import com.hongda.gmall.realtime.util.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    /*在Java中，当一个类继承了一个抽象类并实现了一个接口时，它需要满足以下要求：

    1. 如果抽象类中有任何抽象方法（即没有实现体的方法），那么子类必须提供这些抽象方法的具体实现。
    在您的例子中，DimAsyncFunction继承了RichAsyncFunction抽象类，所以它需要实现RichAsyncFunction中的所有抽象方法。
    2. 当一个类实现一个接口时，它必须提供接口中所有方法的具体实现。
    在您的例子中，DimAsyncFunction实现了DimJoinFunction接口，所以它需要实现DimJoinFunction中的所有方法。

    然而，有以下几种情况可能会影响子类需要实现的方法数量：
    1. 如果抽象类已经实现了接口中的某些方法，那么子类不需要再次实现这些方法，除非它需要覆盖这些实现。
    在您的例子中，如果RichAsyncFunction已经实现了DimJoinFunction中的某些方法，那么DimAsyncFunction不需要再实现这些方法，除非它需要提供不同的实现。
    2. 如果接口中的某些方法已经提供了默认实现（使用default关键字），那么实现该接口的类可以选择性地覆盖这些方法。
    在您的例子中，如果DimJoinFunction中的某些方法有默认实现，那么DimAsyncFunction可以按需覆盖这些方法。
    总之，DimAsyncFunction需要实现RichAsyncFunction中的所有抽象方法以及DimJoinFunction中的所有方法（除非它们已经被实现或提供了默认实现）。
    具体实现哪些方法取决于RichAsyncFunction和DimJoinFunction的定义以及您的业务需求。*/

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    /*被覆写的方法*/
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        /*专门负责IO操作的线程池, 分配固定的CPU字段, 去完成IO操作*/
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    /*抽象方法的实现*/
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //1.查询维表数据
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input)); /*旁路缓存 + 反射创建对象!!!!!!!!!!*/

                //2.将维表数据补充到JavaBean中
                if (dimInfo != null) {
                    join(input, dimInfo);
                }

                //3.将补充之后的数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    /*被覆写的方法*/
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //再次查询补充信息
        System.out.println("TimeOut:" + input);
    }
}

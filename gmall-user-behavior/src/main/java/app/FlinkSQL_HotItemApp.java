package app;


import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhengyonghong
 * @create 2020--12--19--22:18
 */
public class FlinkSQL_HotItemApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取TableAPI执行环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        //2.获取table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,bsSettings);

        //读取数据创建流，转换Bean,提取时间戳,过滤pv
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv").map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //3.流转换表
        Table table = tableEnv.fromDataStream(userBehaviorDS, "userId,itemId,categoryId,behavior,timestamp,rt.rowtime");

        //4.滑动窗口,窗口大小1H,滑动步长5m,求每个itemid的count
        Table itemCountTable = table.window(Slide.over("1.hours").every("5.minutes").on("rt").as("rw"))
                .groupBy("itemId,rw")
                .select("itemId,itemId.count as ict,rw.end as windowTime");

        //基于4的结果注册新表
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(itemCountTable, Row.class);
        tableEnv.createTemporaryView("itemCountTable",rowDataStream,"itemId,ict,windowTime");

        //使用over按照窗口分区获取top5
        Table windowOrderTable = tableEnv.sqlQuery("select windowTime,itemId,ict from (select windowTime,itemId,ict,row_number() over(partition by windowTime order by ict desc) rk from itemCountTable) where rk <= 5");

        //打印测试
        tableEnv.toRetractStream(windowOrderTable,Row.class).print("window");


        //执行
        env.execute();

    }

}

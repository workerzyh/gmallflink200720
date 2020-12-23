package app;

import bean.UserBehavior;
import bean.UvCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.scala.function.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @author zhengyonghong
 * @create 2020--12--21--8:58
 */
public class UvCountApp {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据创建流，转换bean，同时提取时间戳
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new UserBehavior(Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4])
                        );
                    }
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UserBehavior> filterDS = userBehaviorDS.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //全量开窗
        AllWindowedStream<UserBehavior, TimeWindow> allWindowedStream = userBehaviorDS.timeWindowAll(Time.hours(1));

        //使用全量窗口函数，将数据放入set去重
      //  allWindowedStream.apply(new UvCountAllWindowFunc())


    }
    public static class UvCountAllWindowFunc implements AllWindowFunction<UserBehavior, UvCount,TimeWindow>{

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<UvCount> out) {
            //创建hashset用于存放id
            HashSet<Long> uids = new HashSet<>();

            //遍历数据，将uid放入set
            Iterator<UserBehavior> iterator = input.iterator();
            while(iterator.hasNext()){
                uids.add(iterator.next().getUserId());
            }

            //输出数据
            out.collect(new UvCount(new Timestamp(window.getEnd()).toString(),(long)uids.size()));
        }
    }
}

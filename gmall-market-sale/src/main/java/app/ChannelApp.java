package app;

import bean.ChannelBehaviorCount;
import bean.MarketUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import source.MarketBehaviorSource;

import java.sql.Timestamp;

/**
 * @author zhengyonghong
 * @create 2020--12--21--14:08
 */
//TODO 每隔5秒钟统计最近一个小时按照渠道（行为）的推广量
public class ChannelApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境、
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

       //读取数据流
        DataStreamSource<MarketUserBehavior> marketUserBehaviorDS = env.addSource(new MarketBehaviorSource());

        //按照渠道和行为分组
        KeyedStream<MarketUserBehavior, Tuple> keyedStream = marketUserBehaviorDS.keyBy("channel", "behavior");

        //开滑动窗口
        WindowedStream<MarketUserBehavior, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.seconds(5));

        //聚合推广量
        SingleOutputStreamOperator<ChannelBehaviorCount> result = windowedStream.aggregate(new ChannelAggFunc(), new ChannelWindowFcun());

        //6.打印结果
        result.print();

        //7.执行
        env.execute();

    }

    public static class ChannelAggFunc implements AggregateFunction<MarketUserBehavior,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class ChannelWindowFcun implements WindowFunction<Long, ChannelBehaviorCount,Tuple,TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {

                out.collect(new ChannelBehaviorCount(tuple.getField(0),tuple.getField(1),new Timestamp(window.getEnd()).toString(),input.iterator().next()));

        }
    }
}

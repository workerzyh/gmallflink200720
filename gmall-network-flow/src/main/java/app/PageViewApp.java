package app;

import bean.PvCount;
import bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Random;

/**
 * @author zhengyonghong
 * @create 2020--12--19--9:58
 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        //将数据转换为kv结构:防止数据倾斜加随机数使每个并行度都有数据
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = filterDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv" + new Random().nextInt(8), 1);
            }
        }).keyBy(0);

        //开滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS = keyedDS.timeWindow(Time.hours(1));

        //滚动聚合每个并行度中每个窗口的浏览量,并获取窗口信息
        SingleOutputStreamOperator<PvCount> aggregateDS = windowDS.aggregate(new PageViewAggFunc(), new PageViewWindowFunc());

        //按照窗口进行分组
        KeyedStream<PvCount, Long> pvCountLongKeyedStream = aggregateDS.keyBy(PvCount::getWindowEnd);

        //聚合累加每个窗口的所有并行度浏览量
        SingleOutputStreamOperator<String> result = pvCountLongKeyedStream.process(new PageViewProcess());

        //打印
        result.print();

        //8.执行
        env.execute();
    }
    public static class PageViewAggFunc implements AggregateFunction<Tuple2<String, Integer>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> value, Long accumulator) {
            return accumulator + 1;
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

    //获取窗口信息
    public static class PageViewWindowFunc implements WindowFunction<Long, PvCount,Tuple,TimeWindow>{
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            out.collect(new PvCount(window.getEnd(),input.iterator().next()));
        }
    }

    //聚合累加每个窗口的所有并行度浏览量
    public static class PageViewProcess extends KeyedProcessFunction<Long,PvCount,String>{
        //声明状态
        private ListState<PvCount> pvState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            pvState = getRuntimeContext().getListState(new ListStateDescriptor<PvCount>("pvState",PvCount.class));
        }

        @Override
        public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {
            //将新进数据添加到list状态中
            pvState.add(value);

            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取状态中的数据
            Iterator<PvCount> iterator = pvState.get().iterator();

            //遍历求出总数count
            int count  = 0;
            while (iterator.hasNext()){
                count += iterator.next().getCount();
            }

            //休息
            Thread.sleep(200);

            //输出结果
            out.collect("窗口" + new Timestamp(timestamp) + " 的浏览量为：" + count);

            //清空状态
            pvState.clear();

        }
    }
}

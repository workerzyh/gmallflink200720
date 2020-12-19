package app;

import bean.ItemCount;
import bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author zhengyonghong
 * @create 2020--12--19--9:58
 */
public class HotItemApp {
    public static void main(String[] args) throws Exception {
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

        //3.按照商品id分组
        KeyedStream<UserBehavior, Long> itemKeyedStream = filterDS.keyBy(UserBehavior::getItemId);

        //4.滑动窗口，1h窗口大小，5m滑动步长
        WindowedStream<UserBehavior, Long, TimeWindow> windowDS = itemKeyedStream.timeWindow(Time.hours(1), Time.minutes(5));

        //5.聚合计算，计算窗口内部每个商品被点击次数（滚动聚合，保证来一条计算一条，
        // 窗口函数，保证可以拿到窗口时间）
        SingleOutputStreamOperator<ItemCount> aggregateDS = windowDS.aggregate(new ItemAggFunc(), new ItemCountWindowFunc());

        //6.按照窗口时间分组
        KeyedStream<ItemCount, Long> windowEndKeyedStream = aggregateDS.keyBy(ItemCount::getWindowEnd);

        //7.使用processFunction实现收集每个窗口中的数据进行排序输出（状态编程list，定时器）
        SingleOutputStreamOperator<String> topNresult = windowEndKeyedStream.process(new TopnProcess(5));

        //打印测试
        topNresult.print();

        //8.执行
        env.execute();
    }
    public static class ItemAggFunc implements AggregateFunction<UserBehavior,Long,Long>{


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }
    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount,Long, TimeWindow>{

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            //获取key  Item,获取窗口结束时间,获取当前窗口当前item的点击次数
            out.collect(new ItemCount(itemId,window.getEnd(),input.iterator().next()));



        }
    }

    //使用processFunction实现收集每个窗口中的数据进行排序输出（状态编程list，定时器）
    public static class TopnProcess extends KeyedProcessFunction<Long,ItemCount,String>{
        //定义topsize
        private int topSize;

        public TopnProcess() {
        }

        public TopnProcess(int topSize) {
            this.topSize = topSize;
        }

        //声明list状态
        private ListState<ItemCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            itemState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("itemState",ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //将新数据添加到List中
            itemState.add(value);

            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取状态中的数据
            Iterator<ItemCount> iterator = itemState.get().iterator();

            //排序
            ArrayList<ItemCount> itemCList = Lists.newArrayList(iterator);
            itemCList.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if (o1.getCount() > o2.getCount()){
                        return -1;
                    }else if(o1.getCount() < o2.getCount()){
                        return 1;
                    } else {
                        return 0;
                    }             }
            });

            //定义SB用于存放输出结果
            StringBuilder resultStr = new StringBuilder();
            //添加窗口时间
            resultStr.append("==============").append(new Timestamp(timestamp-1000L)).append("==============").append("\n");

            //输出topn
            for (int i = 0; i < Math.min(topSize,itemCList.size()); i++) {
                resultStr.append("top").append(i+1);
                resultStr.append(" itemId:").append(itemCList.get(i).getItemId());
                resultStr.append(" count:").append(itemCList.get(i).getCount());
                resultStr.append("\n");
            }

            //每个窗口休息一下
            Thread.sleep(2000);

            out.collect(resultStr.toString());

            //清空状态
            itemState.clear();

        }
    }
}

package app;

import bean.ApacheLog;
import bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * @author zhengyonghong
 * @create 2020--12--19--14:16
 */
//TODO 每隔5秒，输出最近10分钟内访问量最多的前N个URL
public class HotUrlApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文本数据转换Bean提取时间戳设置水位线
        SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.readTextFile("input/apache.log")
        //SingleOutputStreamOperator<ApacheLog> apacheLogDS = env.socketTextStream("hadoop102",9999)
                .map(new MapFunction<String, ApacheLog>() {
                    @Override
                    public ApacheLog map(String value) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        String[] split = value.split(" ");
                        return new ApacheLog(split[0],
                                split[1],
                                sdf.parse(split[3]).getTime(),
                                split[5], split[6]);
                    }
                }).filter(new FilterFunction<ApacheLog>() {
                    @Override
                    public boolean filter(ApacheLog value) throws Exception {
                        return "GET".equals(value.getMethod());
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        //按照url分组
        KeyedStream<ApacheLog, String> apacheLogKeyedStream = apacheLogDS.keyBy(ApacheLog::getUrl);

        //开滑动窗口，允许迟到1min
        WindowedStream<ApacheLog, String, TimeWindow> windowDS = apacheLogKeyedStream.timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1));

        //计算每个窗口内部url访问次数，滚动聚合，提取窗口信息
        SingleOutputStreamOperator<UrlCount> aggregateDS = windowDS.aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());

        //按照窗口信息重新分组，因为要求每个窗口的累加值
        KeyedStream<UrlCount, Long> windowEndKeyedStream = aggregateDS.keyBy(UrlCount::getWindowEnd);

        //使用process函数实现每个窗口中数据的排序，(状态编程，定时器)
        SingleOutputStreamOperator<String> result = windowEndKeyedStream.process(new UrlTopnProcess(5));

        //打印测试
        result.print();

        //执行
        env.execute();
    }
    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator+1;
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

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlCount,String,TimeWindow>{

        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlCount> out) throws Exception {
            out.collect(new UrlCount(url,window.getEnd(),input.iterator().next()));
        }
    }

    //使用process函数实现每个窗口中数据的排序并输出前topN，状态编程，定时器
    public static class UrlTopnProcess extends KeyedProcessFunction<Long,UrlCount,String>{
        private int urlTopN;

        public UrlTopnProcess() {
        }

        public UrlTopnProcess(int urlTopN) {
            this.urlTopN = urlTopN;
        }
        //声明状态
        private MapState<String,UrlCount> urlState;

        @Override
        public void open(Configuration parameters) throws Exception {
            urlState = getRuntimeContext().getMapState(new MapStateDescriptor<String, UrlCount>("urlState",String.class,UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            //进来的数据添加到状态中
            urlState.put(value.getUrl(),value);

            //注册计算定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);

            //注册清空状态的定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
           //如果是清空状态的，直接清空返回
            if(timestamp == ctx.getCurrentKey()+ 60000L){
                urlState.clear();
                return;
            }

            //触发计算定时器
            //获取状态数据
            Iterator<Map.Entry<String, UrlCount>> iterator = urlState.entries().iterator();

            ArrayList<Map.Entry<String, UrlCount>> entries = Lists.newArrayList(iterator);

            //排序
            entries.sort(new Comparator<Map.Entry<String, UrlCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlCount> o1, Map.Entry<String, UrlCount> o2) {
                    UrlCount urlCount1 = o1.getValue();
                    UrlCount urlCount2 = o2.getValue();
                    if (urlCount1.getCount() > urlCount2.getCount()){
                        return -1;
                    }else if(urlCount1.getCount() < urlCount2.getCount()){
                        return 1;

                    }else{
                        return 0;
                    }

                }
            });

            //取TOPN
            StringBuffer urlStr = new StringBuffer();
            urlStr.append("=============").append(new Timestamp(timestamp)).append("==============").append("\n");

            for (int i = 0; i < Math.min(urlTopN,entries.size()); i++) {
                Map.Entry<String, UrlCount> stringUrlCountEntry = entries.get(i);
                UrlCount urlcount = stringUrlCountEntry.getValue();
                urlStr.append("top").append(i+1)
                        .append(" url:").append(urlcount.getUrl())
                        .append(" count:").append(urlcount.getCount())
                        .append("\n");
            }
            //睡眠
            Thread.sleep(2000);

            //输出
            out.collect(urlStr.toString());
        }
    }
}

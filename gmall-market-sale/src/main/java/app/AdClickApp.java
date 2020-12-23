package app;

import bean.AdClickEvent;
import bean.AdCountByProvince;
import bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
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
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/**
 * @author zhengyonghong
 * @create 2020--12--21--14:27
 */
//TODO 每5s计算近一个小时的，按照省份统计广告点击量，（黑名单：某用户点击某广告次数超过100时发送预警并并且不计入统计）
public class AdClickApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据创建流并传唤为bean，同时提取时间戳，生成watermark
        SingleOutputStreamOperator<AdClickEvent> adClickEventDS = env.readTextFile("input/AdClickLog.csv").map(new MapFunction<String, AdClickEvent>() {
            @Override
            public AdClickEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdClickEvent(Long.parseLong(split[0]), Long.parseLong(split[1])
                        , split[2], split[3], Long.parseLong(split[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
            @Override
            public long extractAscendingTimestamp(AdClickEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //添加过滤逻辑，单日某人点击某广告达到100次加入黑名单（今天后续不再统计该用户点击信息）
        KeyedStream<AdClickEvent, Tuple> userkeyedStream = adClickEventDS.keyBy("userId", "adId");
        SingleOutputStreamOperator<AdClickEvent> filterDS = userkeyedStream.process(new BlickListProcess(100L));

        //分组
        KeyedStream<AdClickEvent, Tuple> keyedStream = filterDS.keyBy("province");

        //开窗
        WindowedStream<AdClickEvent, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.hours(1), Time.seconds(5));

        //计算
        SingleOutputStreamOperator<AdCountByProvince> result = windowedStream.aggregate(new AdAggFunc(), new AdWindowFunc());

        //打印结果
        result.print();
        filterDS.getSideOutput(new OutputTag<BlackListWarning>("balckList"){}).print("黑名单");


        //执行
        env.execute();

    }
    public static class AdAggFunc implements AggregateFunction<AdClickEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class AdWindowFunc implements WindowFunction<Long, AdCountByProvince,Tuple,TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {
            out.collect(new AdCountByProvince(tuple.toString(),new Timestamp(window.getEnd()).toString(),input.iterator().next()));

        }
    }

    public static class BlickListProcess extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent>{

        //定义拉黑人数
        private Long maxClick;

        public BlickListProcess() {
        }

        public BlickListProcess(Long maxClick) {
            this.maxClick = maxClick;
        }

        //声明用户点击广告次数状态
        private ValueState<Long> adCount;

        //声明是否输出到侧输出流状态标记
        private ValueState<Boolean> isSlideOut;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            adCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("adcount-state",Long.class));
            isSlideOut = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("slideout-state",Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            //获取次数状态数据
            Long count = adCount.value();

            //获取侧输出流数据
            Boolean slideState = isSlideOut.value();

            //如果是第一条则直接增加count
            if(count == null){
                adCount.update(1L);

                //注册定时器用于第二天凌晨清空状态
                long ts = ((value.getTimestamp()) / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - 8 * 60 * 60 * 1000L;
                System.out.println(ts);
                ctx.timerService().registerEventTimeTimer(ts);

            }
            //如果不是第一条看,先增加，增加后看是否大于拉黑上限，如果达到且未输出到车输出流，输出预警信息到侧输出流，然后注册第二天清空状态的定时器，，如果已经输出到侧输出流，直接return
            else{
                //更新状态
                adCount.update(count + 1L);

                //判断是否需要拉入黑名单写入侧输出流
                if(count + 1 >= maxClick){
                    if (slideState == null){
                        //拉入黑名单信息输出到侧输出流
                        ctx.output(new OutputTag<BlackListWarning>("balckList"){},
                                new BlackListWarning(value.getUserId(),value.getAdId(),"拉入黑名单"));
                        //侧输出流已发送标记
                        isSlideOut.update(true);
                    }

                    return;
                }
            }

            //输出结果
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            adCount.clear();
            isSlideOut.clear();
        }
    }

}

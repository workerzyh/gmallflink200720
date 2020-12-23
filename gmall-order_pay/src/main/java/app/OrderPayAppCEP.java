package app;

import bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhengyonghong
 * @create 2020--12--22--14:07
 */
//TODO 需求：下单后一段15分钟仍未支付，订单就会被取消,,,使用CEP做
public class OrderPayAppCEP {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取文本数据创建流，转换bean并提取时间戳，
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv").map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000L;
            }
        });

        //按照orderid分组
        KeyedStream<OrderEvent, Long> KeyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //定义模式序列,,15分钟创建+支付
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("next").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //将模式序列作用于流
        PatternStream<OrderEvent> streamPattern = CEP.pattern(KeyedStream, pattern);

        //提取满足模式的事件,匹配上的和超时的都要
        SingleOutputStreamOperator<String> result = streamPattern.select(new OutputTag<String>("timeout") {
        }, new TimeOutFunc(), new PatternSelectFunc());

        //打印正常15分钟内支付数据
        result.print();

        //打印超时数据
        result.getSideOutput(new OutputTag<String>("timeout") {
        }).print("超时支付或未支付数据");


        //执行
        env.execute();
    }

    //超时输出
    public static class TimeOutFunc implements PatternTimeoutFunction<OrderEvent, String> {

        @Override
        public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
            OrderEvent create = map.get("start").get(0);
            return "订单" + create.getOrderId() + "在15分钟内未支付";
        }
    }

    //匹配到的正常输出
    public static class PatternSelectFunc implements PatternSelectFunction<OrderEvent, String> {

        @Override
        public String select(Map<String, List<OrderEvent>> map) throws Exception {
            OrderEvent create = map.get("start").get(0);

            OrderEvent pay = map.get("next").get(0);
            return "订单"+ create.getOrderId() + "创建时间：" + create.getEventTime() +
                    "；支付时间：" + pay.getEventTime();
        }
    }
}

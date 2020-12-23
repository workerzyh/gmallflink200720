package app;

import bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhengyonghong
 * @create 2020--12--22--14:25
 */
//TODO 需求：下单后一段15分钟仍未支付，订单就会被取消,,,使用状态编程做
public class OrderPayAPPState {
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
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //使用状态编程实现
        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderPayProcess());

        //打印结果
        result.print("15分钟内支付订单");

        //打印侧输出流结果
        result.getSideOutput(new OutputTag<String>("orderNotPay"){}).print("创建未支付");
        result.getSideOutput(new OutputTag<String>("orderPayOut15"){}).print("订单创建超出15分钟后支付");
        //执行
        env.execute();
    }

    public static class OrderPayProcess extends KeyedProcessFunction<Long,OrderEvent,String>{
        //声明存储创建订单的状态
        private ValueState<OrderEvent> orderState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-create",OrderEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            //获取状态数据
            OrderEvent orderValue = orderState.value();

            //当是创建数据
            if("create".equals(value.getEventType())){
               //更新状态
                orderState.update(value);

                //注册待支付的定时器
                long ts = (value.getEventTime() + 900) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);

                //更新时间状态
                tsState.update(ts);

            }else if("pay".equals(value.getEventType())){
                Long tsValue = tsState.value();
                //若订单创建状态不空
                if(orderValue != null){
                    //订单创建时间
                    Long creteTime = orderValue.getEventTime();
                    //订单支付时间
                    Long payTime = value.getEventTime();
                    //若支付和创建时间差小于15分钟
                    if(payTime - creteTime <= 900){
                       //输出订单信息
                        out.collect("订单"+ value.getOrderId() + "创建时间：" + creteTime +
                                "；支付时间：" + payTime);

                        //删除定时器
                        ctx.timerService().deleteEventTimeTimer(tsValue);

                        //清空状态
                        tsState.clear();
                        orderState.clear();
                    }

                }else{
                    ctx.output(new OutputTag<String>("orderPayOut15"){},"订单"+value.getOrderId()+"15分钟后支付");
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取创建订单状态
            OrderEvent orderValue = orderState.value();
            //输出到侧输出流
            ctx.output(new OutputTag<String>("orderNotPay"){},"订单"+orderValue.getOrderId()+"未支付");

            //清空状态
            tsState.clear();
            orderState.clear();
        }
    }

}

package app;

import bean.OrderEvent;
import bean.ReceiptEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhengyonghong
 * @create 2020--12--22--15:11
 */
//TODO 需求：订单支付和到账两条流的交易匹配，利用connect实现
public class OrderReceiptWithConnect {
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
        }).filter(new FilterFunction<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000L;
            }
        });

        //第二条流
        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, ReceiptEvent>() {
                    @Override
                    public ReceiptEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new ReceiptEvent(split[0],
                                split[1],
                                Long.parseLong(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //按照事务Id分组之后进行连接
        ConnectedStreams<OrderEvent, ReceiptEvent> connectDS = orderEventDS.keyBy(OrderEvent::getTxId).connect(receiptEventDS.keyBy(ReceiptEvent::getTxId));

        //使用process处理两个流的数据
        SingleOutputStreamOperator<String> result = connectDS.process(new OrderReceiptProcess());

        //打印主流
        result.print("支付并到账");

        //打印侧输出流
        result.getSideOutput(new OutputTag<String>("receipt-output"){}).print("到账未支付侧输出流");
        result.getSideOutput(new OutputTag<String>("order-output"){}).print("支付未到账侧输出流");

        //启动
        env.execute();

    }
    public static class OrderReceiptProcess extends CoProcessFunction<OrderEvent, ReceiptEvent,String>{
        //声明支付和到账状态
        private ValueState<OrderEvent> orderState;
        private ValueState<ReceiptEvent> receiptState;

        //声明定时器时间戳状态
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state",OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state",ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
        }

        //支付流
        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {

            //获取到账和定时器状态数据
            ReceiptEvent receiptValue = receiptState.value();
            Long tsValue = tsState.value();
            //当到账状态不为空，则合并输出到主流，当到账状态为空，则将自己存入支付状态，注册定时器
            if(receiptValue != null){
                //合并输出
                out.collect("支付并已到账，支付时间：" + value.getEventTime() + ", 到账时间：" + receiptValue.getTimestamp() );

                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsValue);

                //清空状态
                receiptState.clear();
                tsState.clear();
            }else{
                //将自己存入支付状态
                orderState.update(value);

                //注册定时器
                long ts = (value.getEventTime() + 5) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);

                //更新定时器状态
                tsState.update(ts);
            }
        }

        //到账流
        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<String> out) throws Exception {
            //获取支付和定时器状态数据
            OrderEvent orderValue = orderState.value();
            Long tsValue = tsState.value();
            //当到账状态不为空，则合并输出到主流，当到账状态为空，则将自己存入支付状态，注册定时器
            if(orderValue != null){
                //合并输出
                out.collect("支付并已到账，支付时间：" + orderValue.getEventTime() + ", 到账时间：" + value.getTimestamp() );

                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsValue);

                //清空状态
                orderState.clear();
                tsState.clear();
            }else{
                //将自己存入到账状态
                receiptState.update(value);

                //注册定时器
                long ts = (value.getTimestamp() + 3) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);

                //更新定时器状态
                tsState.update(ts);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //根据状态数据判断是哪个定时器
            OrderEvent orderTimerValue = orderState.value();
            ReceiptEvent receiptTimerValue = receiptState.value();

            //如果支付为空，则是到账定时器响了，否则就是支付定时器响
            if(orderTimerValue == null){
                //侧输出流输出数据
                ctx.output(new OutputTag<String>("receipt-output"){},receiptTimerValue.getTxId() + "未支付但已到账");

                //清空状态
                receiptState.clear();
            }else{
                //侧输出流输出数据
                ctx.output(new OutputTag<String>("order-output"){},orderTimerValue.getTxId() + "已支付但未到账");

                //清空状态
                orderState.clear();
            }
        }
    }
}

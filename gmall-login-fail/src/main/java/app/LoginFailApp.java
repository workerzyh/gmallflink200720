package app;

import bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.swing.*;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author zhengyonghong
 * @create 2020--12--21--20:07
 */
//TODO 需求：如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
public class LoginFailApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //读取数据创建流转换javabean并提取时间戳设置水位线
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv").map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //按照用户分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        //利用process判断是否存在恶意登录情况
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginFailProcess(2));

        //打印结果
        result.print();
        
        //启动
        env.execute();

    }

    //TODO 2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
    public static class LoginFailProcess extends KeyedProcessFunction<Long,LoginEvent,String>{
        //声明属性
        private int maxSeconds;

        public LoginFailProcess(int maxSeconds) {
            this.maxSeconds = maxSeconds;
        }

        //声明状态，存放连续登录失败的信息
        private ListState<LoginEvent> loginFailState;

        //声明定时器时间戳状态
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态信息
            loginFailState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginfail-state",LoginEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            //获取两个状态信息的值
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginFailState.get().iterator());
            Long tsStateHis = tsState.value();

            //如果数据是失败的
            if("fail".equals(value.getEventType())){
                //添加到list状态中
                loginFailState.add(value);
                //如果第一条失败的
                if(loginEvents.size()==0){
                    //获取时间戳
                    Long curTs = (value.getTimestamp() + maxSeconds) * 1000L;

                    //注册定时器
                    ctx.timerService().registerEventTimeTimer(curTs);

                    //更新定时器时间戳状态
                    tsState.update(curTs);
                }
            }else{
                //成功的话，如果有定时器要删除定时器，并且清空状态
                if(tsStateHis != null){
                    ctx.timerService().deleteEventTimeTimer(tsStateHis);
                }

                //清空状态
                loginFailState.clear();
                tsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //获取登录失败状态数据
            ArrayList<LoginEvent> loginfailList= Lists.newArrayList(loginFailState.get().iterator());

            //预警
            if(loginfailList.size() >= 2){
                out.collect(ctx.getCurrentKey() + "在"+
                        loginfailList.get(0).getTimestamp()+"到"+
                        loginfailList.get(loginfailList.size()-1).getTimestamp()+"时间段内连续登录失败" +
                        loginfailList.size() + "次");
            }

            //清空状态信息
            loginFailState.clear();
            tsState.clear();
        }
    }
}

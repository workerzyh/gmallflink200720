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

import java.util.ArrayList;

/**
 * @author zhengyonghong
 * @create 2020--12--21--20:07
 */
//TODO 需求：如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，输出相关的信息进行报警提示
public class LoginFailApp2 {
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

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

        }

        //声明状态，存放连续登录失败的信息


    }
}

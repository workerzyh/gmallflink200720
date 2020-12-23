package app;

import bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author zhengyonghong
 * @create 2020--12--22--10:28
 */
public class LoginFailWithCep2 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        //TODO 定义模式序列(循环模式),采用简单条件，，两秒内连续两次失败
        Pattern<LoginEvent, LoginEvent> parttern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).times(2).within(Time.seconds(2));

        //模式作用于流
        PatternStream<LoginEvent> cepPattern = CEP.pattern(keyedStream, parttern);

        //提取事件
        SingleOutputStreamOperator<String> selectResult = cepPattern.select(new MyPatternSelectFunc());

        //打印测试
        selectResult.print();

        //启动
        env.execute();
    }
    public static class  MyPatternSelectFunc implements PatternSelectFunction<LoginEvent, String> {

        @Override
        public String select(Map<String, List<LoginEvent>> map) throws Exception {
            //获取事件数据
            List<LoginEvent> start = map.get("start");
            LoginEvent startEvent = start.get(0);
            LoginEvent lastEvent = start.get(start.size()-1);

            //输出预警信息
            return startEvent.getUserId() + "在" +
                    startEvent.getTimestamp() + "到" +
                    lastEvent.getTimestamp() + "之间登录失败"+start.size()+"次";
        }
    }
}

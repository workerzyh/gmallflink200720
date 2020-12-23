package app;

import bean.UserBehavior;
import bean.UvCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

/**
 * @author zhengyonghong
 * @create 2020--12--21--8:58
 */
public class UvCountApp2 {
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

        //全量开窗
        AllWindowedStream<UserBehavior, TimeWindow> allWindowedStream = filterDS.timeWindowAll(Time.hours(1));

        //自定义触发器，实现来一条则计算一条,使用process计算数据
        SingleOutputStreamOperator<UvCount> result = allWindowedStream.trigger(new MyTrigger()).process(new UvProcess());

        //打印测试
        result.print();

        //执行
        env.execute();
    }
    public static class MyTrigger extends Trigger<UserBehavior,TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {


            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UvProcess extends ProcessAllWindowFunction<UserBehavior,UvCount,TimeWindow> {
        //声明redis连接
        private Jedis jedisClient;

        //定义每小时用户访问量的hashkey
        private String hourUvRedisKey;

        //声明布隆过滤器
        private MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedisClient = new Jedis("hadoop102",6379);
            hourUvRedisKey = "HourUv";
            myBloomFilter = new MyBloomFilter(1 << 30);//10亿bit位
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {

            //获取窗口结束时间
            Timestamp windowEnd = new Timestamp(context.window().getEnd());

            //定义位图的rediskey,每个窗口的key
            String bitMapRedisKey = "BitMap" + windowEnd;

            //定义每个小时用户访问量
            String field = windowEnd.toString();//hash中的二层key

            //获取当前uid对应得位置信息
            long offset = myBloomFilter.getOffset(elements.iterator().next().getUserId().toString());

            //查询uid对应位置是否位1
            Boolean getbit = jedisClient.getbit(bitMapRedisKey, offset);

            //如果为flase，则表示该用户第一次访问，
            if(!getbit){
                //将Bit位置为true
                jedisClient.setbit(bitMapRedisKey,offset,true);

                //将窗口访问人数加1，
                jedisClient.hincrBy(hourUvRedisKey,field,1L);
            }

            //如果为true(用户已存在)，直接发送结果
            out.collect(new UvCount(field,Long.parseLong(jedisClient.hget(hourUvRedisKey,field))));

        }

        @Override
        public void close() throws Exception {
            jedisClient.close();
        }
    }

    //自定义布隆过滤器
    public static class MyBloomFilter {
        //定义容量属性，需要传入2的整次幂
        private long cap;

        public MyBloomFilter(long cap) {
            this.cap = cap;
        }

        public MyBloomFilter() {
        }

        //hash函数,获取位图位置信息
        public long getOffset(String value){
            long result = 0;
            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            //取模
            return result & (cap -1);

        }


    }
}

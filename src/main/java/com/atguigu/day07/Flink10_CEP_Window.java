package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink10_CEP_Window {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据并转为JavaBean，以及指定WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env.readTextFile("input/sensor3.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000;
//                                        return element.getTs();
                                    }
                                })
                );

        //TODO 3.定义模式
        Pattern<WaterSensor, WaterSensor> pattern =
                //TODO 模式组
                Pattern
                        .<WaterSensor>begin("start")
                        .where(new IterativeCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                                return "sensor_1".equals(value.getId());
                            }
                        })
//                        .followedBy()
                        .next("end")
                        .where(new IterativeCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
                                return "sensor_2".equals(value.getId());
                            }
                        })
                        //窗口长度
                        .within(Time.seconds(2))
                ;

        //TODO 4.将模式作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorStream, pattern);

        //TODO 5.获取符合规则的数据
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        });

        result.print();

        env.execute();
    }
}

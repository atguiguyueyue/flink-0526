package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JSON格式的字符串
        SingleOutputStreamOperator<String> result = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return JSON.toJSONString(waterSensor);
            }
        });

       //TODO 将数据写到Redis
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        result.addSink(new RedisSink<>(flinkJedisPoolConfig, new RedisMapper<String>() {
            //指定用什么类型来存储数据
            @Override
            public RedisCommandDescription getCommandDescription() {

                //additionalKey:指的是Redis大Key
                return new RedisCommandDescription(RedisCommand.HSET, "0526");
//                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(String data) {
                WaterSensor waterSensor = JSON.parseObject(data, WaterSensor.class);
                return waterSensor.getId();
            }

            @Override
            public String getValueFromData(String data) {
                return data;
            }
        }));

        env.execute();
    }
}

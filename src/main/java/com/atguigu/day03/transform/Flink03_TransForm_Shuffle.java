package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_TransForm_Shuffle {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //2.获取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.过滤出偶数的数据
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);
        //Shuffle
        DataStream<String> shuffle = map.shuffle();

        map.print("原始数据").setParallelism(2);
        shuffle.print("shuffle之后的数据");

        env.execute();
    }
}

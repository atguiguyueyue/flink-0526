package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //2.获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.过滤出偶数的数据
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).setParallelism(2);

        KeyedStream<String, String> keyedStream = flatMap.keyBy(r -> r);

        flatMap.print("原始数据").setParallelism(2);
        keyedStream.print("KeyBy之后的数据");

        env.execute();
    }
}

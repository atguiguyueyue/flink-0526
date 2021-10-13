package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e");

        //3.连接两条流
        ConnectedStreams<Integer, String> connect = integerDataStreamSource.connect(stringDataStreamSource);

        //4.处理两条流
        connect.process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {

                out.collect(value + 1 + "");
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {

                out.collect(value + 1);
            }
        }).print();


        env.execute();
    }
}

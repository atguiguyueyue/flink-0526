package com.atguigu.day02.TransForm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        //3.使用Map方法，将每个数据乘2
        SingleOutputStreamOperator<Integer> mapStream = streamSource.map(new MapFunction<Integer, Integer>() {
            //map方法来一条计算一条
            @Override
            public Integer map(Integer value) throws Exception {
                System.out.println("11111111111");
                return value * 2;
            }
        });

        mapStream.print();

        env.execute();

    }
}

package com.atguigu.day02.TransForm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_TransForm_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.从文件中获取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);


        //3.使用FlatMap方法对每一行数据按照空格切分获取每个单词
        SingleOutputStreamOperator<String> faltMapStream = streamSource.flatMap(new MyFlatMap());


        faltMapStream.print();

        env.execute();

    }

    //自定义一个类继承富函数抽象类
    public static class MyFlatMap extends RichFlatMapFunction<String,String>{
        /**
         * 默认声明周期，调用时间：最先调用，每个并行度调用一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] strings = value.split(" ");
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            for (String s : strings) {
                out.collect(s);
            }
        }

        /**
         * 默认生命周期：调用时间：最后调用，每个并行度调用一次（读文件时每个并行度调用两次）
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}

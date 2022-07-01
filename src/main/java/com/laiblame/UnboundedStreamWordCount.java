package com.laiblame;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * UnboundedStreamWordCount
 *
 * @Description TODO
 * @Date 2022/7/1 8:57
 * @Author shi.ren.gang
 * @Email shirengang@bluestone-ev.com
 */
public class UnboundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        ParameterTool params = ParameterTool.fromArgs(args);

        String hostname = params.get("host");
        Integer port = params.getInt("port");

        DataStreamSource<String> lineDataStreamSource = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG)).
                keyBy(data -> data.f0).sum(1);

        sum.print();

        env.execute();
    }

}

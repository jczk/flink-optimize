package org.wuwangfu.com.tuning.analysis;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.wuwangfu.com.source.MockSourceFunction;
import org.wuwangfu.com.tuning.function.NewMidRichMapFunc;

import java.time.Duration;
import java.util.Properties;


public class PvDemo {


    public static void main(String[] args) throws Exception {

//        Configuration conf = new Configuration();
//        conf.set(RestOptions.ENABLE_FLAMEGRAPH, true);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        
        SingleOutputStreamOperator<JSONObject> jsonobjDS = env
                .addSource(new MockSourceFunction())
                .map(data -> JSONObject.parseObject(data));


        // 按照mid分组，新老用户修正
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonobjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new NewMidRichMapFunc());

        jsonWithNewFlagDS.print();

        env.execute();
    }
}

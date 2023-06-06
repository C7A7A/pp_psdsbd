package com.example.bigdata;

import com.example.bigdata.connectors.Connectors;
import com.example.bigdata.model.SensorData;
import com.example.bigdata.model.SensorDataAgg;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SensorDataAnalysis {
    public static void main(String[] args) throws Exception {

        ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile("src/main/resources/flink.properties");
        ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);
        ParameterTool properties = propertiesFromFile.mergeWith(propertiesFromArgs);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.fromSource(
                Connectors.getFileSource(properties),
                WatermarkStrategy.noWatermarks(),
                "SensorData"
        );

//                fromElements(
//                        "73,altitude,1591531560000",
//                        "63,cadence,1591531560000",
//                        "83,heartrate,1591531560000",
//                        "21,rotations,1591531560000",
//                        "33,speed,1591531560000",
//                        "26,temperature,1591531560000",
//                        "73,altitude,1591531561000",
//                        "63,cadence,1591531561000",
//                        "83,heartrate,1591531561000",
//                        "21,rotations,1591531561000",
//                        "33,speed,1591531561000",
//                        "26,temperature,1591531561000");


        DataStream<SensorData> sensorDataDS = inputStream.map((MapFunction<String, String[]>) txt -> txt.split(",") )
                .filter(array -> array.length == 3)
                .filter(array -> array[0].matches("\\d+") && array[2].matches("\\d+"))
                .map(array -> new SensorData(Integer.parseInt(array[0]), array[1], Long.parseLong(array[2])));

//        sensorDataDS.print();

        DataStream<SensorDataAgg> sensorDataExtDS = sensorDataDS
                .map(sd -> new SensorDataAgg(
                        sd.getSensor(),
                        sd.getValue(), sd.getTimestamp(), // max
                        sd.getValue(), sd.getTimestamp(), // min
                        1, sd.getValue()));

        KeyedStream<SensorDataAgg, String> dataKeyedBySensor = sensorDataExtDS.keyBy(
                SensorDataAgg::getSensor
        );

        DataStream<SensorDataAgg> result = dataKeyedBySensor.reduce(new MyReduceFunction());

        result.addSink(Connectors.getMySQLSink(properties));
//        result.print();

        env.execute("SensorDataAnalysis");
    }
}

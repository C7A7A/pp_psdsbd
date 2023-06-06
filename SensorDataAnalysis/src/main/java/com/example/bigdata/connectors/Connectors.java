package com.example.bigdata.connectors;

import com.example.bigdata.model.SensorDataAgg;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

public class Connectors {
    public static FileSource<String> getFileSource(ParameterTool properties) {
        return FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(properties.getRequired("fileInput.uri")))
                .monitorContinuously(Duration.ofMillis(
                        Long.parseLong(properties.getRequired("fileInput.interval"))))
                .build();
    }

    public static SinkFunction<SensorDataAgg> getMySQLSink(ParameterTool properties) {
        JdbcStatementBuilder<SensorDataAgg> statementBuilder =
            new JdbcStatementBuilder<SensorDataAgg>() {
                @Override
                public void accept(PreparedStatement ps, SensorDataAgg data) throws SQLException {
                    ps.setString(1, data.getSensor());
                    ps.setInt(2, data.getMaxVal());
                    ps.setLong(3, data.getMaxValTimestamp());
                    ps.setInt(4, data.getMinVal());
                    ps.setLong(5, data.getMinValTimestamp());
                    ps.setInt(6, data.getCountVal());
                    ps.setInt(7, data.getSumVal());
                }
            };

            JdbcConnectionOptions connectionOptions = new
                    JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(properties.getRequired("mysql.url"))
                    .withDriverName("com.mysql.jdbc.Driver")
                    .withUsername(properties.getRequired("mysql.username"))
                    .withPassword(properties.getRequired("mysql.password"))
                    .build();

            JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(200)
                    .withMaxRetries(5)
                    .build();

            SinkFunction<SensorDataAgg> jdbcSink =
                    JdbcSink.sink("insert into sensor_data_sink" +
                                    "(sensor, max_val, max_val_timestamp, " +
                                    "min_val, min_val_timestamp, count_val, sum_val) \n" +
                                    "values (?, ?, ?, ?, ?, ?, ?)",
                            statementBuilder,
                            executionOptions,
                            connectionOptions);
        return jdbcSink;
    }

}

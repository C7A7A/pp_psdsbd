import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
public class HouseStatsApp {
    // POJO classes
    static public class InputScores {
        public String house;
        public String character;
        public String score;
        public String ts;
    }
    static public class HouseAlerts {
        public String house;
        public String start_ts;
        public String end_ts;
        public long min_score;
        public long max_score;
    }
    static public class HouseStats {
        public String house;
        public long min_score;
        public long max_score;
    }
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Two parameters are required: boostrapServer threshold");
            System.exit(0);
        }
        final String boostrapServer = args[0];
        final int threshold = Integer.parseInt(args[1]);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "house-alerts-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // setting offset reset to earliest so that we can re-run the code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Serde<InputScores> inputScoresSerde = new JsonPOJOSerde<>(InputScores.class);
        final Serde<HouseStats> houseStatsSerde = new JsonPOJOSerde<>(HouseStats.class);
        final Serde<HouseAlerts> houseAlertsSerde = new JsonPOJOSerde<>(HouseAlerts.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, InputScores> scores = builder.stream("kafka-input", Consumed.with(Serdes.String(), inputScoresSerde));

        KTable<Windowed<String>, HouseStats> stats = scores.groupByKey().
                windowedBy(TimeWindows.of(Duration.ofMinutes(1))).
                aggregate(
                        () -> {
                            HouseStats hs = new HouseStats();
                            hs.house = "";
                            hs.min_score = 9;
                            hs.max_score = 0;
                            return hs;
                        },
                        (aggKey, newValue, aggValue) -> {
                            return aggValue;
                        },
                        Materialized.with(Serdes.String(), houseStatsSerde)
                );
        KStream<String, HouseAlerts> resultStream = stats.toStream().
                        map(
                        (key, value) -> {
                            HouseAlerts ha = new HouseAlerts();
                            ha.house = value.house;
                            ha.start_ts = key.window().startTime().toString();
                            ha.end_ts = key.window().endTime().toString();
                            ha.min_score = value.min_score;
                            ha.max_score = value.max_score;
                            return KeyValue.pair(key.key(), ha);
                        }
                );
        // write to the result topic
        resultStream.to("kafka-output", Produced.with(Serdes.String(), houseAlertsSerde));
        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            final CountDownLatch latch = new CountDownLatch(1);
            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });
            try {
                streams.start();
                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
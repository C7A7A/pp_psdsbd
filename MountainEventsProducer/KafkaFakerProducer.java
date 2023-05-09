import net.datafaker.Faker;
import net.datafaker.fileformats.Csv;
import net.datafaker.fileformats.Format;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaFakerProducer {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 5) {
            System.out.println("Należy podać siedem parametrów: " +
                    "bootstrapServers topicName format noOfRecordsPerSec howLongInSec ");
            System.exit(0);
        }

        Faker faker = new Faker();
        String bootstrapServers = args[0];
        String topicName = args[1];
        String format = args[2];
        int noOfRecordsPerSec = Integer.parseInt(args[3]);
        int howLongInSec = Integer.parseInt(args[4]);

        Properties props = new Properties();
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String record;

        waitToEpoch();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                Random random = new Random();
                ArrayList<String> possibleResults = new ArrayList<>() {
                    {
                        add("summit-reached");
                        add("base-reached");
                        add("resignation-injury");
                        add("resignation-weather");
                        add("resignation-someone-missing");
                        add("resignation-other");
                    }
                };

                String peak_name = faker.mountain().name();
                String mountaineer = faker.mountaineering().mountaineer();

                Timestamp timestamp = faker.date().past(42, TimeUnit.SECONDS);
                timestamp.setNanos(0);
                Timestamp timestampITS = Timestamp.valueOf(LocalDateTime.now().withNano(0));

                if (Objects.equals(format, "csv")) {
                    record = Format.toCsv(
                            Csv.Column.of("peak_name", () -> peak_name),
                            Csv.Column.of("trip_leader", () -> mountaineer),
                            Csv.Column.of("result", () -> possibleResults.get(random.nextInt(possibleResults.size()))),
                            Csv.Column.of("amount_people", () -> String.valueOf(random.nextInt(12) + 1)),
                            Csv.Column.of("ets", timestamp::toString),
                            Csv.Column.of("its", timestampITS::toString))
                        .header(false)
                        .separator(";")
                        .limit(1)
                        .build()
                        .get()
                        .trim();
                } else {
                    record = Format.toJson()
                            .set("peak_name", () -> peak_name)
                            .set("trip_leader", () -> mountaineer)
                            .set("result", () -> possibleResults.get(random.nextInt(possibleResults.size())))
                            .set("amount_people", () -> random.nextInt(12) + 1)
                            .set("ets", timestamp::toString)
                            .set("its", timestampITS::toString)
                            .build()
                            .generate();
                }

                ProducerRecord<String, String> recordToSend = new ProducerRecord<>(topicName, null, timestamp.getTime(), peak_name, record);
                producer.send(recordToSend);
            }
            waitToEpoch();
        }
        producer.close();
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}

package org.example.bigdata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
public class TestProducer {
    public static void main(String[] args) {

        if (args.length < 5) {
            System.out.println("Należy podać pięć parametrów: " + "inputDir sleepTime topicName headerLength bootstrapServers");
            System.exit(0);
        }

        String inputDir = args[0];
        String sleepTime = args[1];
        String topicName = args[2];
        String headerLength = args[3];
        String bootstrapServers = args[4];

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        final File folder = new File(inputDir);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return;
        }

        String[] listOfPaths = Arrays.stream(listOfFiles).
                map(File::getAbsolutePath).toArray(String[]::new);
        Arrays.sort(listOfPaths);

        for (final String fileName : listOfPaths) {
            try (Stream<String> stream = Files.lines(Paths.get(fileName)).skip(Integer.parseInt(headerLength))) {
                stream.forEach(line ->
                        producer.send(new ProducerRecord<>(
                                topicName,
                                String.valueOf(line.hashCode()),
                                line
                        ))
                );

                TimeUnit.SECONDS.sleep(Integer.parseInt(sleepTime));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }
}
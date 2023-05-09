package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;
import net.datafaker.Faker;
import net.datafaker.fileformats.Format;

import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

//        File file = new File("data/data7.txt");
//        PrintStream stream = new PrintStream(file);
//        System.setOut(stream);

        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 30;
            howLongInSec = 6;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;

        try {
            epCompiled = compiler.compile("""
                     @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ets string, its string);
                        
                     create window topTenPeaks#length(10)#unique(peak_name)
                     as (peak_name string, how_many long);
                     
                     insert into topTenPeaks(peak_name, how_many)
                     select peak_name, count(peak_name)
                     from MountainEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 2 sec)
                     group by peak_name
                     order by count(peak_name) desc
                     limit 10;
                     
                     create window lasTopTenPeaks#unique(peak_name)
                     as (peak_name string, present long, how_many long);
                     
                     insert rstream into lasTopTenPeaks(peak_name, present, how_many)
                     select peak_name, count(peak_name), how_many
                     from topTenPeaks
                     group by peak_name;
                     
                     @name('answer')
                     select *
                     from topTenPeaks;
                                     
                    """, compilerArgs);
        }

        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        // Connect to the EPRuntime server and deploy the statement
        EPRuntime runtime = EPRuntimeProvider.getRuntime("http://localhost:port", config);
        EPDeployment deployment;
        try {
            deployment = runtime.getDeploymentService().deploy(epCompiled);
        }
        catch (EPDeployException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "answer");

        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

        Random generator = new Random(2137);
        Faker faker = new Faker(generator);
        String record;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                generateMountainEventData(runtime, faker, generator);
            }
            waitToEpoch();
        }
    }

    private static void generateMountainEventData(EPRuntime runtime, Faker faker, Random random) {
//        Random random = new Random();
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

        String record;
        String name = faker.mountain().name();
        String mountaineer = faker.mountaineering().mountaineer();

        Timestamp timestamp = faker.date().past(42, TimeUnit.SECONDS);
        timestamp.setNanos(0);

        Timestamp timestampITS = Timestamp.valueOf(LocalDateTime.now().withNano(0));

        record = Format.toJson()
                .set("peak_name", () -> name)
                .set("trip_leader", () -> mountaineer)
                .set("result", () -> possibleResults.get(random.nextInt(possibleResults.size())))
                .set("amount_people", () -> random.nextInt(12) + 1)
                .set("ets", timestamp::toString)
                .set("its", timestampITS::toString)
                .build()
                .generate();

        System.out.println(record);
        runtime.getEventService().sendEventJson(record, "MountainEvent");
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}


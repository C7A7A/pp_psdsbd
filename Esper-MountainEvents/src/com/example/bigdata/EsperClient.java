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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
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
            noOfRecordsPerSec = 10;
            howLongInSec = 18;
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
    
                 create table topMountainsLong(peak_name string primary key, how_many long);
                 create table topMountainsShort(peak_name string primary key, how_many long);

                 create window tempMountainLongEv#time_batch(4 sec) as MountainEvent;
                 create window tempMountainShortEv#time_batch(2 sec) as MountainEvent;
                 create window tempMountainDiffEv#keepall as MountainEvent;

                 insert into tempMountainLongEv
                 select *
                 from MountainEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 4 sec);

                 insert into tempMountainShortEv
                 select *
                 from MountainEvent#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 2 sec);

                 insert into tempMountainDiffEv
                 select distinct long.*
                 from tempMountainLongEv as long
                 left outer join tempMountainShortEv as short on long.its = short.its
                 where short.its is null;

                 insert into topMountainsLong
                 select peak_name, count(peak_name) as how_many
                 from tempMountainDiffEv
                 group by peak_name
                 order by count(peak_name) desc
                 limit 10;

                 insert into topMountainsShort
                 select peak_name, count(peak_name) as how_many
                 from tempMountainShortEv
                 group by peak_name
                 order by count(peak_name) desc
                 limit 10;

                 @name('answer')
                    select distinct long.peak_name, long.how_many
                    from MountainEvent#time_batch(2 sec) as ev
                    right outer join topMountainsLong as long on long.peak_name = ev.peak_name
                    left outer join topMountainsShort as short
                    on long.peak_name = short.peak_name
                    where short.peak_name is null
                    order by long.how_many desc
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
        Faker faker = new Faker();
        String record;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                generateMountainEventData(runtime, faker);
            }
            waitToEpoch();
        }
    }

    private static void generateMountainEventData(EPRuntime runtime, Faker faker) {
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


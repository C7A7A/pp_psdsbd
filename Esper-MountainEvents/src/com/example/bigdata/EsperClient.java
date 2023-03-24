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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 30;
            howLongInSec = 10;
        } else {
            noOfRecordsPerSec = Integer.parseInt(args[0]);
            howLongInSec = Integer.parseInt(args[1]);
        }

        Configuration config = new Configuration();
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;
        /*
        1. Pokazuj informację ilu osobom udało się wejść na szczyt lub zdobyć bazę w ciągu ostatnich 10 sekund dodatkowo grupując dane po rezultacie wyprawy.
            """
            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
            @name('result')
                select amount_people, result, sum(amount_people) as sum_people, ts
                from MountainEvent(result IN ('summit-reached', 'base-reached'))#time(10)
                group by result;
                """

        2. Wykrywaj przypadki rezygnacji ze wspinaczki z powodu zaginięcia (resignation someone missing) dla wypraw, w których bierze udział mniej niż 3 osoby
            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
            @name('result')
                select peak_name, trip_leader, result, amount_people
                from MountainEvent(result = "resignation-someone-missing")#length(10)
                having amount_people < 3;"""

        3. Wykrywaj przypadki rezygnacji ze wspinaczki dla grup, w których liczba osób jest największa w ostatnich 20 wyprawach (batch)
            """
            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
            @name('result')
                select peak_name, trip_leader, result, amount_people, max(amount_people) as max_people, ts
                from MountainEvent(result IN ("resignation-someone-missing", "resignation-injury", "resignation-weather", "resignation-other"))
                    #length_batch(20)
                having max(amount_people) = amount_people;"""

        4. Dla każdego alpinisty (istnieje 10 alpinistów w zbiorze danych) znajdź pierwszą wyprawę, która zakończyła się kontuzją.
        Następnie wykrywaj każdą kolejną wyprawę, która zakończyła się kontuzją oraz w wyprawie wzięło udział więcej osób niż w pierwszej nieudanej wyprawie.
            """
           @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
            @name('createWindow')
                create window MountainEventFirstFailed#length(10) as MountainEvent;

            @name('fillWindow')
                insert into MountainEventFirstFailed
                select *
                from MountainEvent(result = "resignation-injury")#firstunique(trip_leader);

            @name('result')
                select meff.trip_leader, meff.result, meff.amount_people, me.trip_leader, me.amount_people
                from MountainEvent(result = "resignation-injury")#length(1) as me, MountainEventFirstFailed as meff
                where me.amount_people > meff.amount_people and me.trip_leader = meff.trip_leader;"""
        */
        try {
            epCompiled = compiler.compile("""
            @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
            @name('createWindow')
                create window MountainEventFirstFailed#length(10) as MountainEvent;
                
            @name('fillWindow')
                insert into MountainEventFirstFailed
                select *
                from MountainEvent(result = "resignation-injury")#firstunique(trip_leader);
                
            @name('result')
                select meff.trip_leader, meff.result, meff.amount_people, me.trip_leader, me.amount_people
                from MountainEvent(result = "resignation-injury")#length(1) as me, MountainEventFirstFailed as meff
                where me.amount_people > meff.amount_people and me.trip_leader = meff.trip_leader;
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

        EPStatement resultStatement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "result");
//        EPStatement resultStatement2 = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "result2");
//        EPStatement resultStatement3 = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "result3");

        // Add a listener to the statement to handle incoming events
//        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
//            for (EventBean eventBean : newData) {
//                System.out.printf("R: %s%n", eventBean.getUnderlying());
//            }
//        });
//
//        resultStatement2.addListener( (newData, oldData, stmt, runTime) -> {
//            for (EventBean eventBean : newData) {
//                System.out.printf("R: %s%n", eventBean.getUnderlying());
//            }
//        });

        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });

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

    private static void generateScoreEventData(EPRuntime runtime, Faker faker) {
        String record;
        String house = faker.harryPotter().house();
        Timestamp timestamp = faker.date().past(30, TimeUnit.SECONDS);
        record = Format.toJson()
                .set("house", () -> house)
                .set("character", () -> faker.harryPotter().character())
                .set("score", () -> String.valueOf(faker.number().randomDigitNotZero()))
                .set("ts", timestamp::toString)
                .build().generate();
        runtime.getEventService().sendEventJson(record, "ScoreEvent");
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
        Timestamp timestamp = faker.date().past(30, TimeUnit.SECONDS);

        record = Format.toJson()
                .set("peak_name", () -> name)
                .set("trip_leader", () -> mountaineer)
                .set("result", () -> possibleResults.get(random.nextInt(possibleResults.size())))
                .set("amount_people", () -> random.nextInt(12) + 1)
                .set("ts", timestamp::toString)
                .build()
                .generate();
        runtime.getEventService().sendEventJson(record, "MountainEvent");
    }

    private static EPCompiled selectScoreEvents(CompilerArguments compilerArgs, EPCompiler compiler) throws EPCompileException {
        return compiler.compile("""
                        @public @buseventtype create json schema ScoreEvent(house string, character string, score int, ts string);
                        @name('result')
                            select house, character, score, avg(score) as avgScore
                            from ScoreEvent#length(10)
                            having score > avg(score);""", compilerArgs);
    }

    private static EPCompiled selectMountainEvents(CompilerArguments compilerArgs, EPCompiler compiler) throws EPCompileException {
        return compiler.compile("""
                @public @buseventtype create json schema MountainEvent(peak_name string, trip_leader string, result string, amount_people int, ts string);
                @name('result')
                    select * from MountainEvent;
                """, compilerArgs);
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}


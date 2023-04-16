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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class EsperClient {
    public static void main(String[] args) throws InterruptedException {
        int noOfRecordsPerSec;
        int howLongInSec;
        if (args.length < 2) {
            noOfRecordsPerSec = 10;
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
        try {
//            epCompiled = compiler.compile("""
//                    @public @buseventtype create json schema ScoreEvent(house string, character string, score int, ts string);
//                    @name('result') SELECT * from ScoreEvent.win:time(10 sec)
//                    group by house
//                    having score > avg(score);""", compilerArgs);
//            String epl_1 = """
//                    @public @buseventtype create json schema JokeEvent(character string, quote string, people_in_room int, laughing_people int, ts string);
//                    @name('result') SELECT character, avg(laughing_people), ts from JokeEvent.win:time(10 sec)
//                    group by character;""";

            String epl_1 = """
                    @public @buseventtype create json schema JokeEvent(
                        character string, quote string, people_in_room int, laughing_people int, pub string, ets string, its string
                    );
                    create window BadJokesWin#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
                                                (people_in_room int, laughing_people int, pub string, its string, ets string);
                                        
                    insert into BadJokesWin
                    select people_in_room, laughing_people, pub, its, ets
                    from JokeEvent
                    where (laughing_people * 2) < people_in_room;
                                
                    create window FunJokesWin#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
                        (people_in_room int, laughing_people int, pub string, its string, ets string);
                                
                    insert into FunJokesWin
                    select people_in_room, laughing_people, pub, its, ets
                    from JokeEvent
                    where (laughing_people * 2) > people_in_room;
                                
                    @name('answer')
                    select bad.pub, min(bad.its) as its_start
                    from BadJokesWin bad
                    full outer join FunJokesWin fun
                    on fun.pub = bad.pub
                    group by bad.pub
                    having count(bad.pub) > count(fun.pub);
                                        
                    """;
            epCompiled = compiler.compile(epl_1, compilerArgs);

        }
        catch (EPCompileException ex) {
            // handle exception here
            throw new RuntimeException(ex);
        }

        /*
        select bad.pub, bad.its as its_start, count(bad.pub), count(fun.pub), fun.pub
                    from FunJokesWin fun
                    full outer join BadJokesWin bad
                    on fun.pub = bad.pub
                    group by bad.pub
                    having count(bad.pub) > count(fun.pub);
        1.
        @name('answer')
            select character, avg(laughing_people) as avglaughing_people
            from JokeEvent
            group by character;

        2.
        @name('answer')
            select character, its
            from JokeEvent
            where laughing_people = 0;

        3.
        @name('answer')
            select character, laughing_people, its
            from JokeEvent#ext_timed(java.sql.Timestamp.valueOf(its).getTime(), 2 min)
            having laughing_people = max(laughing_people);

        4.
        create window BadJokesWin#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
                                                (people_in_room int, laughing_people int, pub string, its string, ets string);

        insert into BadJokesWin
        select people_in_room, laughing_people, pub, its, ets
        from JokeEvent
        where (laughing_people * 2) < people_in_room;

        create window FunJokesWin#ext_timed_batch(java.sql.Timestamp.valueOf(its).getTime(), 5 sec)
            (people_in_room int, laughing_people int, pub string, its string, ets string);

        insert into FunJokesWin
        select people_in_room, laughing_people, pub, its, ets
        from JokeEvent
        where (laughing_people * 2) > people_in_room;

        @name('answer')
        select bad.pub, min(bad.its) as its_start
        from BadJokesWin bad
        full outer join FunJokesWin fun
        on fun.pub = bad.pub
        group by bad.pub
        having count(bad.pub) > count(fun.pub);

        5.
        @name('answer')
            select * from JokeEvent
                match_recognize (
                    partition by pub
                    measures
                        B.pub as pub,
                        A[0].its as its_start
                    pattern (A{2, } B)
                    define
                        A as A.laughing_people <= 5 and A.pub = "McLaren's Pub",
                        B as B.laughing_people > 5 and B.pub = "McLaren's Pub"
                )

        6.
        @name('answer')
            select a.laughing_people as laughing_people1, b.people_in_room, b.laughing_people as laughing_people2, d.laughing_people as laughing_people3
            from pattern[
                every (a=JokeEvent(a.laughing_people > 4) -> (
                        b=JokeEvent(b.laughing_people > 4 and b.people_in_room < 30) and not c=JokeEvent(c.people_in_room >= 30)) -> (
                        d=JokeEvent(d.laughing_people > 4) and not e=JokeEvent(e.people_in_room >= 30))
                )
            ];

        7.
        @name('answer')
            select * from JokeEvent
            match_recognize (
                partition by pub
                measures
                    A.pub as pub,
                    A.laughing_people as ppl_before,
                    last(B.laughing_people) as ppl_after,
                    count(B.pub) as joke_count
                pattern (A B{4, } C)
                define
                    B as B.laughing_people < A.laughing_people and B.laughing_people < prev(B.laughing_people),
                    C as C.laughing_people > B.LastOf().laughing_people
            )
        */

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

        // Add a listener to the statement to handle incoming events
        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.printf("R: %s%n", eventBean.getUnderlying());
            }
        });
        Faker faker = new Faker(new Random(25));
        String record;

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + (1000L * howLongInSec)) {
            for (int i = 0; i < noOfRecordsPerSec; i++) {
                String character = faker.howIMetYourMother().character();
                String[] pubs = {"McLaren's Pub", "McGee's Pub", "Pemberton's Pub", "Flaming Saddles Saloon",
                        "As Is NYC", "Lilly's Craft", "Empanada Mama", "Southgate", "P&J Carney's Pub", "O'Donoghue's"};
                Random random = new Random();
                int index = random.nextInt(pubs.length);
                String selectedPub = pubs[index];

                Timestamp eTimestamp = faker.date().past(30, TimeUnit.SECONDS);
                eTimestamp.setNanos(0);
                Timestamp iTimestamp = Timestamp.valueOf(LocalDateTime.now().withNano(0));

                int people_in_room = faker.number().numberBetween(0,10);
                record = Format.toJson()
                        .set("character", () -> character)
                        .set("quote", () -> faker.howIMetYourMother().quote())
                        .set("people_in_room", () -> String.valueOf(people_in_room))
                        .set("laughing_people", () -> String.valueOf(faker.number().numberBetween(0,people_in_room)))
                        .set("pub", () -> selectedPub)
                        .set("ets", eTimestamp::toString)
                        .set("its", () -> iTimestamp.toString())
                        .build().generate();
                runtime.getEventService().sendEventJson(record, "JokeEvent");
            }
            waitToEpoch();
        }
    }

    static void waitToEpoch() throws InterruptedException {
        long millis = System.currentTimeMillis();
        Instant instant = Instant.ofEpochMilli(millis) ;
        Instant instantTrunc = instant.truncatedTo( ChronoUnit.SECONDS ) ;
        long millis2 = instantTrunc.toEpochMilli() ;
        TimeUnit.MILLISECONDS.sleep(millis2+1000-millis);
    }
}


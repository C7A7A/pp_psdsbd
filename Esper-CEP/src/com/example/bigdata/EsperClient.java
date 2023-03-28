package com.example.bigdata;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

public class EsperClient {

    public static void main(String[] args) {

        Configuration config = new Configuration();
        CompilerArguments compilerArgs = new CompilerArguments(config);

        // Compile the EPL statement
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPCompiled epCompiled;

        /*

        create window AcmeTicker#length(10) as Ticker;
        insert into AcmeTicker select * from Ticker where symbol = 'ACME';

        1.
        @name('result') select a.* from pattern[ every a=AcmeTicker(price=25) ]

        2.
        @name('result')
        select a.tstamp, b.price - a.price as diff, b.tstamp
        from pattern[
            every (a=AcmeTicker -> b=AcmeTicker)
        ]

        3.
        @name('result')
        select a.tstamp, b.price - a.price as diff, b.tstamp
        from pattern[
            every (a=AcmeTicker -> b=AcmeTicker)
        ]
        where b.price - a.price < 0

        4.
        @name('result')
        select a.tstamp, a.price, b.price, b.tstamp
        from pattern[
            every a=AcmeTicker(a.price > 23) -> b=AcmeTicker(b.price > 23)
        ]

        5.
        @name('result')
        select a.tstamp, a.price, b.price, b.tstamp
        from pattern[
            every a=AcmeTicker(a.price > 23) -> (
                not AcmeTicker(price < 14) and b=AcmeTicker(b.price > 23)
            )
        ]

        6.
        select a[0].tstamp time_start, a[0].price a1, a[1].price a2, a[2].price a3
        from pattern[
            every ([3:] (a=AcmeTicker(price >= 20) ) until AcmeTicker(price < 20))
        ]

        7.
        select * from AcmeTicker
        match_recognize (
            partition by symbol
            measures
                A.price as Aprice, min(B.price) as minBPrice, last(C.price) as maxCPrice,
                A.tstamp as startTime, last(C.tstamp) as stopTime
            pattern (A B+ C+)
            define
                B as B.price < A.price,
                C as C.price > B.LastOf().price and C.price > prev(C.price)
        )

        8.
        @public @buseventtype create json schema Ticker(symbol string, tstamp string, price int);

        create window AllTicker#length(10) as Ticker;
        insert into AllTicker select * from Ticker;

        @name('result')
        select * from AllTicker
        match_recognize (
            partition by symbol
            measures
                A.symbol as symbol,
                A.price as Aprice, min(B.price) as minBPrice, last(C.price) as maxCPrice,
                A.tstamp as startTime, last(C.tstamp) as stopTime
            pattern (A B+ C+)
            define
                B as B.price < A.price,
                C as C.price > B.LastOf().price and C.price > prev(C.price)
            )

        9.
        select * from AllTicker
        match_recognize (
            partition by symbol
            measures
                A.symbol as symbol,
                A.price as Aprice, B.price as BPrice, C.price as CPrice,
                A.tstamp as firstTime, B.tstamp as secondTime, C.tstamp as thirdTime
            pattern (A B C)
            define
                B as B.price > A.price,
                C as (C.price - B.price) > (B.price - A.price) * 2
            )

        10.
        @public @buseventtype create json schema Ticker(symbol string, tstamp string, price int);

        create window AllTicker#length(100) as Ticker;
        insert into AllTicker select * from Ticker;

        @name('result')
        select * from AllTicker
        match_recognize (
            partition by symbol
            measures
                A.symbol as symbol,
                A.price as Aprice, min(B.price) as minBPrice, last(C.price) as maxCPrice, min(D.price) as minDPrice, last(E.price) as maxEPrice,
                A.tstamp as startTime, last(E.tstamp) as stopTime
            pattern (A B+ C+ D+ E+)
            define
                B as B.price < A.price and B.price < prev(B.price),
                C as C.price > B.LastOf().price and C.price > prev(C.price),
                D as D.price < C.LastOf().price and D.price < prev(D.price),
                E as E.price > D.LastOf().price and E.price > prev(E.price)
            )
        */
        try {
            epCompiled = compiler.compile("""
                    @public @buseventtype create json schema Ticker(symbol string, tstamp string, price int);
                    
                    create window AllTicker#length(100) as Ticker;
                    insert into AllTicker select * from Ticker;
                    
                    @name('result')
                    select * from AllTicker
                    match_recognize (
                        partition by symbol
                        measures
                            A.symbol as symbol,
                            A.price as Aprice, min(B.price) as minBPrice, last(C.price) as maxCPrice, min(D.price) as minDPrice, last(E.price) as maxEPrice,
                            A.tstamp as startTime, last(E.tstamp) as stopTime
                        pattern (A B+ C+ D+ E+)
                        define
                            B as B.price < A.price and B.price < prev(B.price),
                            C as C.price > B.LastOf().price and C.price > prev(C.price),
                            D as D.price < C.LastOf().price and D.price < prev(D.price),
                            E as E.price > D.LastOf().price and E.price > prev(E.price)
                        )
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


        // Add a listener to the statement to handle incoming events
        // String processing and making JsonObject out of it
        resultStatement.addListener( (newData, oldData, stmt, runTime) -> {
            for (EventBean eventBean : newData) {
                System.out.println(eventBean.getUnderlying());
            }
        });

        for (String s : createInputData()) {
            runtime.getEventService().sendEventJson(s, "Ticker");
        }
    }

    static String[] createInputData() {
        return new String[] {
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-01 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-01 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-01 00:00:00.0\", \"price\":22}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-02 00:00:00.0\", \"price\":17}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-02 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-02 00:00:00.0\", \"price\":22}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-03 00:00:00.0\", \"price\":19}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-03 00:00:00.0\", \"price\":13}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-03 00:00:00.0\", \"price\":19}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-04 00:00:00.0\", \"price\":21}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-04 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-04 00:00:00.0\", \"price\":18}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-05 00:00:00.0\", \"price\":25}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-05 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-05 00:00:00.0\", \"price\":17}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-06 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-06 00:00:00.0\", \"price\":10}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-06 00:00:00.0\", \"price\":20}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-07 00:00:00.0\", \"price\":15}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-07 00:00:00.0\", \"price\":9}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-07 00:00:00.0\", \"price\":17}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-08 00:00:00.0\", \"price\":20}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-08 00:00:00.0\", \"price\":8}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-08 00:00:00.0\", \"price\":20}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-09 00:00:00.0\", \"price\":24}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-09 00:00:00.0\", \"price\":9}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-09 00:00:00.0\", \"price\":16}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-10 00:00:00.0\", \"price\":25}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-10 00:00:00.0\", \"price\":9}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-10 00:00:00.0\", \"price\":15}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-11 00:00:00.0\", \"price\":19}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-11 00:00:00.0\", \"price\":9}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-11 00:00:00.0\", \"price\":15}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-12 00:00:00.0\", \"price\":15}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-12 00:00:00.0\", \"price\":9}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-12 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-13 00:00:00.0\", \"price\":25}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-13 00:00:00.0\", \"price\":10}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-13 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-14 00:00:00.0\", \"price\":25}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-14 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-14 00:00:00.0\", \"price\":15}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-15 00:00:00.0\", \"price\":14}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-15 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-15 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-16 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-16 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-16 00:00:00.0\", \"price\":16}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-17 00:00:00.0\", \"price\":14}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-17 00:00:00.0\", \"price\":8}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-17 00:00:00.0\", \"price\":14}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-18 00:00:00.0\", \"price\":24}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-18 00:00:00.0\", \"price\":7}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-18 00:00:00.0\", \"price\":12}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-19 00:00:00.0\", \"price\":23}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-19 00:00:00.0\", \"price\":5}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-19 00:00:00.0\", \"price\":11}",
                "{\"symbol\":\"ACME\", \"tstamp\":\"2011-04-20 00:00:00.0\", \"price\":22}",
                "{\"symbol\":\"GLOBEX\", \"tstamp\":\"2011-04-20 00:00:00.0\", \"price\":3}",
                "{\"symbol\":\"OSCORP\", \"tstamp\":\"2011-04-20 00:00:00.0\", \"price\":9}"
        };
    }
}


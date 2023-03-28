import java.io.IOException;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.getCommon().addEventType(KursAkcji.class);
        EPRuntime epRuntime = EPRuntimeProvider.getDefaultRuntime(configuration);

        /*
        5.
        select istream data, spolka, kursZamkniecia, max(kursZamkniecia) - kursZamkniecia as roznica
        from KursAkcji#ext_timed_batch(data.getTime(), 1 days);"""

        6.
        """select istream data, spolka, kursZamkniecia, max(kursZamkniecia) - kursZamkniecia as roznica
        from KursAkcji(spolka in ('IBM', 'Honda', 'Microsoft'))#ext_timed_batch(data.getTime(), 1 days);"""

        7.
        a)
        """
        select irstream data, kursOtwarcia, kursZamkniecia, (kursZamkniecia - kursOtwarcia) as roznica, spolka
        from KursAkcji((kursZamkniecia - kursOtwarcia) > 0)#ext_timed(data.getTime(), 1 days);"""
        b)
        """
        select irstream data, kursOtwarcia, kursZamkniecia, (kursZamkniecia - kursOtwarcia) as roznica, spolka
        from KursAkcji(Main.differenceStocks(kursOtwarcia, kursZamkniecia) > 0)#ext_timed(data.getTime(), 1 days);"""

        8.
        """
        select istream data, kursZamkniecia, (max(kursZamkniecia) - kursZamkniecia) as roznica, spolka
        from KursAkcji(spolka in ('PepsiCo', 'CocaCola'))#ext_timed(data.getTime(), 7 days);"""

        9.
        """
        select istream data, kursZamkniecia, max(kursZamkniecia) as MaxKursZamkniecia, spolka
        from KursAkcji(spolka in ('PepsiCo', 'CocaCola'))#ext_timed_batch(data.getTime(), 1 days)
        having max(kursZamkniecia) = kursZamkniecia;"""

        10.
        """
        select istream max(kursZamkniecia) as maksimum
        from KursAkcji#ext_timed_batch(data.getTime(), 7 days)"""

        11.
        """
        select cc.kursZamkniecia as kurscc,
                pc.kursZamkniecia as kurspc,
                pc.kursZamkniecia - cc.kursZamkniecia as roznica,
                pc.data
        from KursAkcji(spolka = 'CocaCola')#length(1) as cc
                full outer join KursAkcji(spolka = 'PepsiCo')#length(1) as pc
                on cc.data = pc.data
        where (pc.kursZamkniecia - cc.kursZamkniecia) > 0
        """

        12.
        1)
        create window StartKursAkcji#length(2) as KursAkcji;

        insert into StartKursAkcji
        select *
        from KursAkcji(spolka in ('CocaCola', 'PepsiCo'))
        having data = min(data);

        select ka.spolka, ka.data, ka.kursZamkniecia - ska.kursZamkniecia as roznica
        from KursAkcji#length(2) as ka, StartKursAkcji as ska
        where ka.spolka = ska.spolka;

        2)
        select a.data, a.spolka, a.kursZamkniecia as kursBiezacy, a.kursZamkniecia - b.kursZamkniecia as roznica
        from KursAkcji#length(1) as a join KursAkcji(spolka in ('CocaCola', 'PepsiCo'))#firstunique(spolka) as b
        on a.spolka = b.spolka;

        13.
        create window StartKursAkcji#length(11) as KursAkcji;

        insert into StartKursAkcji
        select *
        from KursAkcji
        having data = min(data);

        select ka.spolka, ka.data, ka.kursZamkniecia - ska.kursZamkniecia as roznica
        from KursAkcji#length(1) as ka, StartKursAkcji as ska
        where ka.spolka = ska.spolka and ka.kursZamkniecia - ska.kursZamkniecia > 0;

        14.
        select b.spolka, b.data, a.data, a.kursOtwarcia, b.kursOtwarcia, ((a.data.getTime() - b.data.getTime()) / (1000 * 60 * 60 * 24)) AS days_diff
                from pattern [
                    every a=KursAkcji -> (
                        every b=KursAkcji(
                            b.spolka = a.spolka and
                            (b.kursOtwarcia - a.kursOtwarcia) not between 3.0 and -3.0 and
                            ((a.data.getTime() - b.data.getTime()) / (1000 * 60 * 60 * 24)) between 6.0 and -6.0
                        )
                    )
                ]
        15.
        IMO TO JEST ZROBIONE W INNY SPOSÓB NIŻ BYŁO ZAPLANOWANE XD
        select data, spolka, obrot
                from KursAkcji#ext_timed_batch(data.getTime(), 7 days)
                where market = "NYSE"
                order by obrot desc
                limit 3

        16.
        select data, spolka, obrot
                from KursAkcji#ext_timed_batch(data.getTime(), 7 days)
                where market = "NYSE"
                order by obrot desc
                limit 3 offset 3

        */
        String query = """
                select data, spolka, obrot
                    from KursAkcji#ext_timed_batch(data.getTime(), 7 days)
                    where market = "NYSE"
                    order by obrot desc
                    limit 1 offset 2;
                """;
        EPDeployment deployment = compileAndDeploy(epRuntime, query);

        ProstyListener prostyListener = new ProstyListener();
        for (EPStatement statement : deployment.getStatements()) {
            statement.addListener(prostyListener);
        }

        InputStream inputStream = new InputStream();
        inputStream.generuj(epRuntime.getEventService());
    }

    public static EPDeployment compileAndDeploy(EPRuntime epRuntime, String epl) {
        EPDeploymentService deploymentService = epRuntime.getDeploymentService();
        CompilerArguments args =
                new CompilerArguments(epRuntime.getConfigurationDeepCopy());
        EPDeployment deployment;
        try {
            EPCompiled epCompiled = EPCompilerProvider.getCompiler().compile(epl, args);
            deployment = deploymentService.deploy(epCompiled);
        } catch (EPCompileException e) {
            throw new RuntimeException(e);
        } catch (EPDeployException e) {
            throw new RuntimeException(e);
        }
        return deployment;
    }

    public static Float differenceStocks(Float openingRate, Float closingRate) {
        return closingRate - openingRate;
    }
}

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
        1.
        select istream data, spolka, kursZamkniecia, max(kursZamkniecia) - kursZamkniecia as roznica
        from KursAkcji#ext_timed_batch(data.getTime(), 1 days);"""

        2.
        """select istream data, spolka, kursZamkniecia, max(kursZamkniecia) - kursZamkniecia as roznica
        from KursAkcji(spolka in ('IBM', 'Honda', 'Microsoft'))#ext_timed_batch(data.getTime(), 1 days);"""
        */

        String query = """
        select istream data, spolka, kursZamkniecia, max(kursZamkniecia) - kursZamkniecia as roznica
        from KursAkcji(spolka in ('IBM', 'Honda', 'Microsoft'))#ext_timed_batch(data.getTime(), 1 days);""";

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
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.tests.product.launcher.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.prestosql.tests.product.launcher.Extensions;
import io.prestosql.tests.product.launcher.LauncherModule;
import io.prestosql.tests.product.launcher.PathResolver;
import io.prestosql.tests.product.launcher.env.EnvironmentOptions;
import io.prestosql.tests.product.launcher.suite.Suite;
import io.prestosql.tests.product.launcher.suite.SuiteConfig;
import io.prestosql.tests.product.launcher.suite.SuiteFactory;
import io.prestosql.tests.product.launcher.suite.SuiteModule;
import io.prestosql.tests.product.launcher.suite.SuiteTestRun;

import javax.inject.Inject;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static io.prestosql.tests.product.launcher.cli.Commands.runCommand;
import static io.prestosql.tests.product.launcher.env.EnvironmentOptions.applySuiteConfig;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Command(name = "describe", description = "describe tests suite")
public class SuiteDescribe
        implements Runnable
{
    private final Module additionalSuites;

    @Inject
    public SuiteDescribeOptions options = new SuiteDescribeOptions();

    public SuiteDescribe(Extensions extensions)
    {
        this.additionalSuites = requireNonNull(extensions, "extensions is null").getAdditionalSuites();
    }

    @Override
    public void run()
    {
        Module suiteModule = new SuiteModule(additionalSuites);
        SuiteConfig config = getSuiteConfig(suiteModule, options.config);

        runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new SuiteModule(additionalSuites))
                        .add(options.toModule())
                        .add(applySuiteConfig(new EnvironmentOptions(), config).toModule())
                        .build(),
                SuiteDescribe.Execution.class);
    }

    private static SuiteConfig getSuiteConfig(Module suiteModule, String configName)
    {
        Injector injector = Guice.createInjector(suiteModule);
        SuiteFactory instance = injector.getInstance(SuiteFactory.class);
        return instance.getSuiteConfig(configName);
    }

    public static class SuiteDescribeOptions
    {
        @Option(name = "--suite", title = "suite", description = "the name of the suite to run", required = true)
        public String suite;

        @Option(name = "--suite-config", title = "suite-config", description = "the name of the suite config to use")
        public String config = "config-default";

        public Module toModule()
        {
            return binder -> binder.bind(SuiteDescribeOptions.class).toInstance(this);
        }
    }

    public static class Execution
            implements Runnable
    {
        private final String suiteName;
        private final String suiteConfig;
        private final SuiteFactory suiteFactory;
        private final EnvironmentOptions environmentOptions;
        private final PathResolver pathResolver;
        private final PrintStream out;

        @Inject
        public Execution(SuiteDescribeOptions describeOptions, SuiteFactory suiteFactory, PathResolver pathResolver, EnvironmentOptions environmentOptions)
        {
            this.suiteName = requireNonNull(describeOptions.suite, "describeOptions.suite is null");
            this.suiteConfig = requireNonNull(describeOptions.config, "describeOptions.config is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");
            this.pathResolver = requireNonNull(pathResolver, "pathResolver is null");
            this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");

            try {
                this.out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            }
            catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Could not create print stream", e);
            }
        }

        @Override
        public void run()
        {
            Suite suite = suiteFactory.getSuite(suiteName);
            SuiteConfig config = suiteFactory.getSuiteConfig(suiteConfig);

            out.println(format("Suite '%s' with configuration '%s' consists of following test runs: ", suiteName, suiteConfig));

            for (SuiteTestRun testRun : suite.getTestRuns(config)) {
                TestRun.TestRunOptions runOptions = createTestRunOptions(suiteName, testRun, config);
                out.println(format("\npresto-product-tests-launcher/bin/run-launcher test run %s\n", OptionsPrinter.format(environmentOptions, runOptions)));
            }
        }

        private TestRun.TestRunOptions createTestRunOptions(String suiteName, SuiteTestRun suiteTestRun, SuiteConfig suiteConfig)
        {
            TestRun.TestRunOptions testRunOptions = new TestRun.TestRunOptions();
            testRunOptions.environment = suiteTestRun.getEnvironmentName();
            testRunOptions.testArguments = suiteTestRun.getTemptoRunArguments(suiteConfig);
            testRunOptions.testJar = pathResolver.resolvePlaceholders(testRunOptions.testJar);
            testRunOptions.reportsDir = format("presto-product-tests/target/%s/%s/%s", suiteName, suiteConfig.getConfigName(), suiteTestRun.getEnvironmentName());
            return testRunOptions;
        }
    }
}

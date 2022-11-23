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
package io.trino.tests.product.launcher.cli;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;
import io.trino.tests.product.launcher.cli.suite.describe.json.JsonOutput;
import io.trino.tests.product.launcher.cli.suite.describe.json.JsonSuite;
import io.trino.tests.product.launcher.cli.suite.describe.json.JsonTestRun;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteFactory;
import io.trino.tests.product.launcher.suite.SuiteModule;
import io.trino.tests.product.launcher.suite.SuiteTestRun;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import javax.inject.Inject;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.trino.tests.product.launcher.cli.Commands.runCommand;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.ExitCode.OK;
import static picocli.CommandLine.Option;

@Command(
        name = "describe",
        description = "Describe tests suite",
        usageHelpAutoWidth = true)
public class SuiteDescribe
        implements Callable<Integer>
{
    private final Module additionalSuites;
    private final Module additionalEnvironments;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public SuiteDescribeOptions options = new SuiteDescribeOptions();

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    public SuiteDescribe(Extensions extensions)
    {
        this.additionalSuites = extensions.getAdditionalSuites();
        this.additionalEnvironments = extensions.getAdditionalEnvironments();
    }

    @Override
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new SuiteModule(additionalSuites))
                        .add(new EnvironmentModule(environmentOptions, additionalEnvironments))
                        .add(options.toModule())
                        .build(),
                SuiteDescribe.Execution.class);
    }

    public enum SuiteDescribeFormat
    {
        TEXT(TextOutputBuilder::new),
        JSON(JsonOutputBuilder::new);

        private final Supplier<OutputBuilder> outputBuilderFactory;

        SuiteDescribeFormat(Supplier<OutputBuilder> outputBuilderFactory)
        {
            this.outputBuilderFactory = requireNonNull(outputBuilderFactory, "outputBuilderFactory is null");
        }
    }

    public static class SuiteDescribeOptions
    {
        private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

        @Option(names = "--suite", paramLabel = "<suite>", description = "Name of the suite(s) to run (comma separated)", required = true, split = ",")
        public List<String> suites;

        @Option(names = "--test-jar", paramLabel = "<jar>", description = "Path to test JAR " + DEFAULT_VALUE, defaultValue = "${product-tests.module}/target/${product-tests.name}-${project.version}-executable.jar")
        public File testJar;

        @Option(names = "--format", description = "Table output format: ${COMPLETION-CANDIDATES} " + DEFAULT_VALUE, defaultValue = "TEXT")
        public SuiteDescribeFormat format;

        public Module toModule()
        {
            return binder -> binder.bind(SuiteDescribeOptions.class).toInstance(this);
        }
    }

    private interface OutputBuilder
    {
        void setConfig(String config);

        void addSuite(String suiteName);

        void addTestRun(EnvironmentOptions environmentOptions, TestRun.TestRunOptions runOptions, Environment environment);

        String build();
    }

    private static class TextOutputBuilder
            implements OutputBuilder
    {
        private StringBuilder sb = new StringBuilder();
        private String config;

        @Override
        public void setConfig(String config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public void addSuite(String suiteName)
        {
            sb.append(String.format("Suite '%s' with configuration '%s' consists of following test runs: %n", suiteName, config));
        }

        @Override
        public void addTestRun(EnvironmentOptions environmentOptions, TestRun.TestRunOptions runOptions, Environment environment)
        {
            sb.append(String.format("%n%s test run %s%n%n", environmentOptions.launcherBin, OptionsPrinter.format(environmentOptions, runOptions)));
        }

        @Override
        public String build()
        {
            return sb.toString();
        }
    }

    private static class JsonOutputBuilder
            implements OutputBuilder
    {
        private static final JsonCodec<JsonOutput> CODEC = new JsonCodecFactory().jsonCodec(JsonOutput.class);

        private String config;
        private ImmutableList.Builder<JsonSuite> suites;
        private JsonSuite lastSuite;

        @Override
        public void setConfig(String config)
        {
            this.config = config;
            this.suites = ImmutableList.builder();
        }

        @Override
        public void addSuite(String suiteName)
        {
            lastSuite = new JsonSuite(suiteName);
            requireNonNull(suites, "suites is null").add(lastSuite);
        }

        @Override
        public void addTestRun(EnvironmentOptions environmentOptions, TestRun.TestRunOptions runOptions, Environment environment)
        {
            JsonTestRun testRun = new JsonTestRun(environment);
            requireNonNull(lastSuite, "lastSuite is null").getTestRuns().add(testRun);
        }

        @Override
        public String build()
        {
            return CODEC.toJson(new JsonOutput(config, suites.build()));
        }
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final SuiteDescribeOptions describeOptions;
        private final String config;
        private final SuiteFactory suiteFactory;
        private final EnvironmentConfigFactory configFactory;
        private final EnvironmentFactory environmentFactory;
        private final EnvironmentOptions environmentOptions;
        private final PrintStream out;
        private final OutputBuilder outputBuilder;

        @Inject
        public Execution(
                SuiteDescribeOptions describeOptions,
                SuiteFactory suiteFactory,
                EnvironmentConfigFactory configFactory,
                EnvironmentFactory environmentFactory,
                EnvironmentOptions environmentOptions)
        {
            this.describeOptions = requireNonNull(describeOptions, "describeOptions is null");
            this.config = requireNonNull(environmentOptions.config, "environmentOptions.config is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");
            this.outputBuilder = describeOptions.format.outputBuilderFactory.get();

            try {
                this.out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            }
            catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Could not create print stream", e);
            }
        }

        @Override
        public Integer call()
        {
            outputBuilder.setConfig(this.config);
            for (String suiteName : describeOptions.suites) {
                Suite suite = suiteFactory.getSuite(suiteName);
                EnvironmentConfig config = configFactory.getConfig(this.config);

                outputBuilder.addSuite(suiteName);
                for (SuiteTestRun testRun : suite.getTestRuns(config)) {
                    testRun = testRun.withConfigApplied(config);
                    TestRun.TestRunOptions runOptions = createTestRunOptions(suiteName, testRun, config);
                    Environment.Builder builder = environmentFactory.get(runOptions.environment, config, testRun.getExtraOptions())
                            .setContainerOutputMode(environmentOptions.output);
                    Environment environment = builder.build();
                    outputBuilder.addTestRun(environmentOptions, runOptions, environment);
                }
            }
            out.print(outputBuilder.build());
            return OK;
        }

        private TestRun.TestRunOptions createTestRunOptions(String suiteName, SuiteTestRun suiteTestRun, EnvironmentConfig environmentConfig)
        {
            TestRun.TestRunOptions testRunOptions = new TestRun.TestRunOptions();
            testRunOptions.environment = suiteTestRun.getEnvironmentName();
            testRunOptions.extraOptions = suiteTestRun.getExtraOptions();
            testRunOptions.testArguments = suiteTestRun.getTemptoRunArguments();
            testRunOptions.testJar = describeOptions.testJar;
            testRunOptions.reportsDir = Paths.get(format("testing/trino-product-tests/target/%s/%s/%s", suiteName, environmentConfig.getConfigName(), suiteTestRun.getEnvironmentName()));
            testRunOptions.startupRetries = null;
            testRunOptions.logsDirBase = Optional.empty();
            return testRunOptions;
        }
    }
}

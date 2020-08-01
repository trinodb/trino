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
package io.prestosql.benchmark.driver;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.units.Duration;
import io.prestosql.benchmark.driver.BenchmarkDriverOptions.ClientExtraCredential;
import io.prestosql.benchmark.driver.BenchmarkDriverOptions.ClientSessionProperty;
import io.prestosql.benchmark.driver.PrestoBenchmarkDriver.VersionProvider;
import io.prestosql.client.ClientSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static java.util.function.Function.identity;

@Command(
        name = "presto-benchmark-driver",
        usageHelpAutoWidth = true,
        versionProvider = VersionProvider.class)
public class PrestoBenchmarkDriver
        implements Callable<Integer>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Option(names = "--version", versionHelp = true, description = "Print version information and exit")
    public boolean versionInfoRequested;

    @Mixin
    public BenchmarkDriverOptions driverOptions;

    public static void main(String[] args)
    {
        new CommandLine(new PrestoBenchmarkDriver())
                .registerConverter(ClientSessionProperty.class, ClientSessionProperty::new)
                .registerConverter(ClientExtraCredential.class, ClientExtraCredential::new)
                .registerConverter(HostAndPort.class, HostAndPort::fromString)
                .registerConverter(Duration.class, Duration::valueOf)
                .execute(args);
    }

    @Override
    public Integer call()
            throws Exception
    {
        initializeLogging(driverOptions.debug);

        // select suites
        List<Suite> suites = Suite.readSuites(new File(driverOptions.suiteConfigFile));
        if (!driverOptions.suites.isEmpty()) {
            suites = suites.stream()
                    .filter(suite -> driverOptions.suites.contains(suite.getName()))
                    .collect(Collectors.toList());
        }
        suites = ImmutableList.copyOf(suites);

        // load queries
        File queriesDir = new File(driverOptions.sqlTemplateDir);
        List<BenchmarkQuery> allQueries = readQueries(queriesDir);

        // select queries to run
        Set<BenchmarkQuery> queries;
        if (driverOptions.queries.isEmpty()) {
            queries = suites.stream()
                    .map(suite -> suite.selectQueries(allQueries))
                    .flatMap(List::stream)
                    .collect(Collectors.toSet());
        }
        else {
            queries = driverOptions.queries.stream()
                    .map(Pattern::compile)
                    .map(pattern -> allQueries.stream().filter(query -> pattern.matcher(query.getName()).matches()))
                    .flatMap(identity())
                    .collect(Collectors.toSet());
        }

        // create results store
        BenchmarkResultsStore resultsStore = getResultsStore(suites, queries);

        // create session
        ClientSession session = driverOptions.getClientSession();

        try (BenchmarkDriver benchmarkDriver = new BenchmarkDriver(
                resultsStore,
                session,
                queries,
                driverOptions.warm,
                driverOptions.runs,
                driverOptions.debug,
                driverOptions.maxFailures,
                Optional.ofNullable(driverOptions.socksProxy))) {
            for (Suite suite : suites) {
                benchmarkDriver.run(suite);
            }
        }

        return 0;
    }

    protected BenchmarkResultsStore getResultsStore(List<Suite> suites, Set<BenchmarkQuery> queries)
    {
        return new BenchmarkResultsPrinter(suites, queries);
    }

    private static List<BenchmarkQuery> readQueries(File queriesDir)
            throws IOException
    {
        File[] files = queriesDir.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        Arrays.sort(files);

        ImmutableList.Builder<BenchmarkQuery> queries = ImmutableList.builder();
        for (File file : files) {
            String fileName = file.getName();
            if (fileName.endsWith(".sql")) {
                queries.add(new BenchmarkQuery(file));
            }
        }
        return queries.build();
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public static void initializeLogging(boolean debug)
    {
        // unhook out and err while initializing logging or logger will print to them
        PrintStream out = System.out;
        PrintStream err = System.err;
        try {
            if (debug) {
                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.setLevel("io.prestosql", Level.DEBUG);
            }
            else {
                System.setOut(new PrintStream(nullOutputStream()));
                System.setErr(new PrintStream(nullOutputStream()));

                Logging logging = Logging.initialize();
                logging.configure(new LoggingConfiguration());
                logging.disableConsole();
            }
        }
        finally {
            System.setOut(out);
            System.setErr(err);
        }
    }

    public static class VersionProvider
            implements IVersionProvider
    {
        @Spec
        public CommandSpec spec;

        @Override
        public String[] getVersion()
        {
            String version = getClass().getPackage().getImplementationVersion();
            return new String[] {spec.name() + " " + firstNonNull(version, "(version unknown)")};
        }
    }
}

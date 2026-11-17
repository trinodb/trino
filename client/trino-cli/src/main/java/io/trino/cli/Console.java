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
package io.trino.cli;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.cli.ClientOptions.OutputFormat;
import io.trino.cli.ClientOptions.PropertyMapping;
import io.trino.cli.Trino.VersionProvider;
import io.trino.cli.lexer.StatementSplitter;
import io.trino.cli.lexer.StatementSplitter.Statement;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.InfoCmp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static io.trino.cli.Completion.commandCompleter;
import static io.trino.cli.Help.getHelpText;
import static io.trino.cli.QueryPreprocessor.preprocessQuery;
import static io.trino.cli.TerminalUtils.closeTerminal;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TerminalUtils.isRealTerminal;
import static io.trino.cli.TerminalUtils.terminalEncoding;
import static io.trino.cli.Trino.formatCliErrorMessage;
import static io.trino.cli.lexer.StatementSplitter.isEmptyStatement;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jline.utils.AttributedStyle.DEFAULT;

@Command(
        name = "trino",
        header = "Trino command line interface",
        synopsisHeading = "%nUSAGE:%n%n",
        optionListHeading = "%nOPTIONS:%n",
        usageHelpAutoWidth = true,
        versionProvider = VersionProvider.class)
public class Console
        implements Callable<Integer>
{
    public static final Set<String> STATEMENT_DELIMITERS = ImmutableSet.of(";", "\\G");

    private static final String PROMPT_NAME = "trino";
    private static final Duration EXIT_DELAY = new Duration(3, SECONDS);

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Option(names = "--version", versionHelp = true, description = "Print version information and exit")
    public boolean versionInfoRequested;

    @Mixin
    public ClientOptions clientOptions;

    @Override
    public Integer call()
    {
        return run() ? 0 : 1;
    }

    public boolean run()
    {
        CommandLine.ParseResult parseResult = spec.commandLine().getParseResult();

        Map<String, String> restrictedOptions = spec.options().stream()
                .filter(parseResult::hasMatchedOption)
                .map(option -> getMapping(option.userObject())
                        .map(value -> new AbstractMap.SimpleEntry<>(value, option.longestName())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

        ConnectionFactory connectionFactory = clientOptions.createConnectionFactory(restrictedOptions);
        boolean hasQuery = clientOptions.execute != null;
        boolean isFromFile = !isNullOrEmpty(clientOptions.file);

        String query = clientOptions.execute;
        if (hasQuery) {
            query += ";";
        }

        if (isFromFile) {
            if (hasQuery) {
                throw new RuntimeException("both --execute and --file specified");
            }
            try {
                query = asCharSource(new File(clientOptions.file), UTF_8).read();
                hasQuery = true;
            }
            catch (IOException e) {
                throw new RuntimeException(format("Error reading from file %s: %s", clientOptions.file, e.getMessage()));
            }
        }

        // Read queries from stdin
        if (!hasQuery && !isRealTerminal()) {
            try {
                if (System.in.available() > 0) {
                    query = new String(System.in.readAllBytes(), terminalEncoding()) + ";";

                    if (query.length() > 1) {
                        hasQuery = true;
                    }
                }
            }
            catch (IOException e) {
                // ignored
            }
        }

        // abort any running query if the CLI is terminated
        AtomicBoolean exiting = new AtomicBoolean();
        ThreadInterruptor interruptor = new ThreadInterruptor();
        CountDownLatch exited = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            exiting.set(true);
            interruptor.interrupt();
            @SuppressWarnings("UnusedLocalVariable")
            var ignored = awaitUninterruptibly(exited, EXIT_DELAY.toMillis(), MILLISECONDS);
            // Terminal closing restores terminal settings and releases underlying system resources
            closeTerminal();
        }));

        Theme theme = clientOptions.theme.resolve(isRealTerminal(), noColorRequested());

        try (QueryRunner queryRunner = createQueryRunner(theme, connectionFactory)) {
            if (hasQuery) {
                return executeCommand(
                        queryRunner,
                        exiting,
                        query,
                        clientOptions.outputFormat,
                        clientOptions.ignoreErrors,
                        clientOptions.progress.orElse(false),
                        clientOptions.decimalDataSize);
            }

            Optional<String> pager = clientOptions.pager;
            runConsole(
                    queryRunner,
                    exiting,
                    clientOptions.outputFormatInteractive,
                    clientOptions.editingMode,
                    getHistoryFile(clientOptions.historyFile),
                    pager,
                    clientOptions.progress.orElse(true),
                    clientOptions.disableAutoSuggestion,
                    clientOptions.decimalDataSize,
                    theme);
            return true;
        }
        finally {
            exited.countDown();
            interruptor.close();
        }
    }

    private QueryRunner createQueryRunner(Theme theme, ConnectionFactory connectionFactory)
    {
        try {
            return new QueryRunner(
                    theme,
                    connectionFactory,
                    clientOptions.debug,
                    clientOptions.maxQueuedRows,
                    clientOptions.maxBufferedRows);
        }
        catch (SQLException e) {
            connectionFactory.close();
            throw new RuntimeException("Error connecting: " + e.getMessage(), e);
        }
    }

    // Honor the https://no-color.org/ convention: any non-empty NO_COLOR disables color.
    private static boolean noColorRequested()
    {
        String value = System.getenv("NO_COLOR");
        return value != null && !value.isEmpty();
    }

    private static Optional<String> getMapping(Object userObject)
    {
        if (userObject instanceof Field) {
            return Optional.ofNullable(((Field) userObject).getAnnotation(PropertyMapping.class))
                    .map(PropertyMapping::value);
        }

        return Optional.empty();
    }

    private static void runConsole(
            QueryRunner queryRunner,
            AtomicBoolean exiting,
            OutputFormat outputFormat,
            ClientOptions.EditingMode editingMode,
            Optional<Path> historyFile,
            Optional<String> pager,
            boolean progress,
            boolean disableAutoSuggestion,
            boolean decimalDataSize,
            Theme theme)
    {
        try (TableNameCompleter tableNameCompleter = new TableNameCompleter(queryRunner);
                InputReader reader = new InputReader(editingMode, historyFile, disableAutoSuggestion, theme, commandCompleter(), tableNameCompleter)) {
            tableNameCompleter.populateCache();
            String remaining = "";
            while (!exiting.get()) {
                // setup prompt
                String prompt = PROMPT_NAME;
                Optional<String> schema = queryRunner.getSchema();
                prompt += schema.map(value -> ":" + value.replace("%", "%%"))
                        .orElse("");
                String commandPrompt = prompt + "> ";

                // read a line of input from user
                String line;
                try {
                    line = reader.readLine(commandPrompt, remaining);
                }
                catch (UserInterruptException e) {
                    if (!e.getPartialLine().isEmpty()) {
                        reader.getHistory().add(e.getPartialLine());
                    }
                    remaining = "";
                    continue;
                }
                catch (EndOfFileException e) {
                    System.out.println();
                    return;
                }

                // check for special commands -- must match InputParser
                String command = CharMatcher.is(';').or(whitespace()).trimTrailingFrom(line);
                switch (command.toLowerCase(ENGLISH)) {
                    case "exit":
                    case "quit":
                        return;
                    case "clear":
                        Terminal terminal = reader.getTerminal();
                        terminal.puts(InfoCmp.Capability.clear_screen);
                        terminal.flush();
                        continue;
                    case "history":
                        for (History.Entry entry : reader.getHistory()) {
                            System.out.println(new AttributedStringBuilder()
                                    .style(theme.historyIndex())
                                    .append(format("%5d", entry.index() + 1))
                                    .style(DEFAULT)
                                    .append("  ")
                                    .append(entry.line())
                                    .toAnsi(reader.getTerminal()));
                        }
                        continue;
                    case "help":
                        System.out.println();
                        System.out.println(getHelpText());
                        continue;
                }

                // execute any complete statements
                StatementSplitter splitter = new StatementSplitter(line, STATEMENT_DELIMITERS);
                for (Statement split : splitter.getCompleteStatements()) {
                    OutputFormat currentOutputFormat = outputFormat;
                    if (split.terminator().equals("\\G")) {
                        currentOutputFormat = OutputFormat.VERTICAL;
                    }

                    process(queryRunner, split.statement(), currentOutputFormat, pager, progress, decimalDataSize, reader.getTerminal(), System.out, System.out);
                }
                tableNameCompleter.populateCache();

                // replace remaining with trailing partial statement
                remaining = whitespace().trimTrailingFrom(splitter.getPartialStatement());
            }
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private static boolean executeCommand(
            QueryRunner queryRunner,
            AtomicBoolean exiting,
            String query,
            OutputFormat outputFormat,
            boolean ignoreErrors,
            boolean showProgress,
            boolean decimalDataSize)
    {
        boolean success = true;
        StatementSplitter splitter = new StatementSplitter(query);
        for (Statement split : splitter.getCompleteStatements()) {
            if (!isEmptyStatement(split.statement())) {
                if (!process(queryRunner, split.statement(), outputFormat, Optional.of(""), showProgress, decimalDataSize, getTerminal(), System.out, System.err)) {
                    if (!ignoreErrors) {
                        return false;
                    }
                    success = false;
                }
            }
            if (exiting.get()) {
                return success;
            }
        }
        if (!isEmptyStatement(splitter.getPartialStatement())) {
            System.err.println("Non-terminated statement: " + splitter.getPartialStatement());
            return false;
        }
        return success;
    }

    private static boolean process(
            QueryRunner queryRunner,
            String sql,
            OutputFormat outputFormat,
            Optional<String> pager,
            boolean showProgress,
            boolean decimalDataSize,
            Terminal terminal,
            PrintStream out,
            PrintStream errorChannel)
    {
        String finalSql;
        try {
            finalSql = preprocessQuery(
                    terminal,
                    queryRunner::getCatalog,
                    queryRunner::getSchema,
                    sql);
        }
        catch (QueryPreprocessorException e) {
            System.err.println(e.getMessage());
            if (queryRunner.isDebug()) {
                e.printStackTrace(System.err);
            }
            return false;
        }

        try (Query query = queryRunner.startQuery(finalSql)) {
            return query.renderOutput(terminal, out, errorChannel, outputFormat, pager, showProgress, decimalDataSize);
        }
        catch (SQLException | RuntimeException e) {
            System.err.println(formatCliErrorMessage(e, queryRunner.theme(), queryRunner.isDebug()));
            return false;
        }
    }

    private static Optional<Path> getHistoryFile(String path)
    {
        if (isNullOrEmpty(path)) {
            return Optional.empty();
        }
        return Optional.of(Path.of(path));
    }
}

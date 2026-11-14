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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.cli.ClientOptions.OutputFormat;
import io.trino.jdbc.TrinoSqlException;
import org.jline.terminal.Terminal;
import org.jline.terminal.Terminal.Signal;
import org.jline.terminal.Terminal.SignalHandler;
import org.jline.utils.AttributedStringBuilder;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER_AND_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.STANDARD;
import static io.trino.cli.TerminalUtils.isRealTerminal;
import static io.trino.cli.Trino.formatCliErrorMessage;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jline.utils.AttributedStyle.DEFAULT;

public class Query
        implements Closeable
{
    private static final long WARNINGS_POLL_INTERVAL_MILLIS = 100;
    private static final Duration CANCELLATION_TIMEOUT = new Duration(3, SECONDS);

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final AtomicBoolean userAborted = new AtomicBoolean();
    private final Statement statement;
    private final String sql;
    private final StatementExecution execution;
    private final Executor executor;
    private final boolean debug;
    private final int maxBufferedRows;
    private final Theme theme;

    public Query(Statement statement, String sql, Executor executor, boolean debug, int maxQueuedRows, int maxBufferedRows, Theme theme)
    {
        this.statement = requireNonNull(statement, "statement is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.execution = new StatementExecution(statement, sql, maxQueuedRows);
        this.executor = requireNonNull(executor, "executor is null");
        this.debug = debug;
        this.maxBufferedRows = maxBufferedRows;
        this.theme = requireNonNull(theme, "theme is null");
    }

    public boolean renderOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, OutputFormat outputFormat, Optional<String> pager, boolean showProgress, boolean decimalDataSize)
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = terminal.handle(Signal.INT, signal -> {
            if (ignoreUserInterrupt.get() || userAborted.get()) {
                return;
            }
            abort();
            clientThread.interrupt();
        });
        try {
            return renderQueryOutput(terminal, out, errorChannel, outputFormat, pager, showProgress, decimalDataSize);
        }
        finally {
            terminal.handle(Signal.INT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private boolean renderQueryOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, OutputFormat outputFormat, Optional<String> pager, boolean showProgress, boolean decimalDataSize)
    {
        StatusPrinter statusPrinter = null;
        WarningsPrinter warningsPrinter = new PrintStreamWarningsPrinter(errorChannel, theme);

        execution.start(executor);
        try {
            if (showProgress) {
                statusPrinter = new StatusPrinter(execution, this::abort, errorChannel, debug, isInteractive(pager), decimalDataSize, theme);
                statusPrinter.printInitialStatusUpdates(terminal);
            }
            else {
                processInitialStatusUpdates(warningsPrinter);
            }

            Optional<String> updateStatus = execution.getUpdateStatus();
            Optional<List<Column>> columns = execution.getColumns();
            if (updateStatus.isPresent()) {
                errorChannel.println(updateStatus.get());
            }
            if (columns.isPresent() && !userAborted.get()) {
                renderResults(terminal, out, outputFormat, pager, columns.get());
            }
            execution.awaitFinished();
        }
        catch (InterruptedException e) {
            abort();
        }

        warningsPrinter.print(execution.getWarnings(), true, true);

        if (showProgress) {
            statusPrinter.printFinalInfo();
        }

        if (userAborted.get()) {
            errorChannel.println("Query aborted by user");
            return false;
        }

        Optional<Throwable> failure = execution.getFailure();
        if (failure.isPresent()) {
            renderFailure(errorChannel, failure.get());
            return false;
        }

        return true;
    }

    private boolean isInteractive(Optional<String> pager)
    {
        return pager.map(name -> !name.trim().isEmpty()).orElse(true);
    }

    private void processInitialStatusUpdates(WarningsPrinter warningsPrinter)
            throws InterruptedException
    {
        while (!execution.awaitResults(WARNINGS_POLL_INTERVAL_MILLIS, MILLISECONDS)) {
            warningsPrinter.print(execution.getWarnings(), true, false);
        }
        warningsPrinter.print(execution.getWarnings(), false, true);
    }

    private void renderResults(Terminal terminal, PrintStream out, OutputFormat outputFormat, Optional<String> pager, List<Column> columns)
    {
        try {
            doRenderResults(terminal, out, outputFormat, pager, columns);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            abort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException e) {
            // reading rows is interrupted when the user aborts the query
            if (!userAborted.get()) {
                throw e;
            }
        }
    }

    private void doRenderResults(Terminal terminal, PrintStream out, OutputFormat format, Optional<String> pager, List<Column> columns)
            throws IOException
    {
        if (isInteractive(pager)) {
            pageOutput(pager, format, terminal.getWidth(), columns);
        }
        else {
            sendOutput(out, format, terminal.getWidth(), columns);
        }
    }

    private void pageOutput(Optional<String> pagerName, OutputFormat format, int maxWidth, List<Column> columns)
            throws IOException
    {
        try (Pager pager = Pager.create(pagerName);
                ThreadInterruptor clientThread = new ThreadInterruptor();
                Writer writer = createWriter(pager);
                OutputHandler handler = createOutputHandler(format, maxWidth, writer, columns)) {
            if (!pager.isNullPager()) {
                // ignore the user pressing ctrl-C while in the pager
                ignoreUserInterrupt.set(true);
                pager.getFinishFuture().thenRun(() -> {
                    ignoreUserInterrupt.set(false);
                    abort();
                    clientThread.interrupt();
                });
            }
            handler.processRows(execution);
        }
        catch (RuntimeException | IOException e) {
            if (userAborted.get() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
        }
    }

    private void sendOutput(PrintStream out, OutputFormat format, int maxWidth, List<Column> columns)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, maxWidth, createWriter(out), columns)) {
            handler.processRows(execution);
        }
    }

    private OutputHandler createOutputHandler(OutputFormat format, int maxWidth, Writer writer, List<Column> columns)
    {
        return new OutputHandler(createOutputPrinter(format, maxWidth, writer, columns), maxBufferedRows);
    }

    private OutputPrinter createOutputPrinter(OutputFormat format, int maxWidth, Writer writer, List<Column> columns)
    {
        List<String> fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        switch (format) {
            case AUTO:
                return new AutoTablePrinter(columns, writer, maxWidth, theme);
            case ALIGNED:
                return new AlignedTablePrinter(columns, writer, theme);
            case VERTICAL:
                return new VerticalRecordPrinter(fieldNames, writer);
            case CSV:
                return new CsvPrinter(fieldNames, writer, NO_HEADER);
            case CSV_HEADER:
                return new CsvPrinter(fieldNames, writer, STANDARD);
            case CSV_UNQUOTED:
                return new CsvPrinter(fieldNames, writer, NO_HEADER_AND_QUOTES);
            case CSV_HEADER_UNQUOTED:
                return new CsvPrinter(fieldNames, writer, NO_QUOTES);
            case TSV:
                return new TsvPrinter(fieldNames, writer, false);
            case TSV_HEADER:
                return new TsvPrinter(fieldNames, writer, true);
            case JSON:
                return new JsonPrinter(fieldNames, writer);
            case MARKDOWN:
                return new MarkdownTablePrinter(columns, writer);
            case NULL:
                return new NullPrinter();
        }
        throw new RuntimeException(format + " not supported");
    }

    private static Writer createWriter(OutputStream out)
    {
        return new BufferedWriter(new OutputStreamWriter(out, UTF_8), 16384);
    }

    @Override
    public void close()
    {
        if (execution.isStarted() && !execution.isFinished()) {
            execution.cancel();
            execution.awaitFinishedUninterruptibly(CANCELLATION_TIMEOUT.toMillis(), MILLISECONDS);
        }
        try {
            statement.close();
        }
        catch (SQLException ignored) {
        }
    }

    /**
     * Cancels the query, unless it already finished.
     */
    private void abort()
    {
        if (!execution.isStarted() || execution.isFinished()) {
            return;
        }
        userAborted.set(true);
        execution.cancel();
    }

    private void renderFailure(PrintStream out, Throwable failure)
    {
        if (!(failure instanceof TrinoSqlException)) {
            out.println(formatCliErrorMessage(failure, theme, debug));
            return;
        }

        TrinoSqlException exception = (TrinoSqlException) failure;
        out.printf("Query %s failed: %s%n", exception.getQueryId(), exception.getErrorMessage());
        if (debug && (exception.getCause() != null)) {
            exception.getCause().printStackTrace(out);
        }
        exception.getErrorLocation().ifPresent(location -> renderErrorLocation(sql, location.getLineNumber(), location.getColumnNumber(), out));
        out.println();
    }

    private void renderErrorLocation(String query, int lineNumber, int columnNumber, PrintStream out)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(lineNumber - 1);
        String good = errorLine.substring(0, columnNumber - 1);
        String bad = errorLine.substring(columnNumber - 1);

        if ((lineNumber == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (isRealTerminal()) {
            AttributedStringBuilder builder = new AttributedStringBuilder();

            builder.style(theme.errorContext());
            for (int i = 1; i < lineNumber; i++) {
                builder.append(lines.get(i - 1)).append("\n");
            }
            builder.append(good);

            builder.style(theme.error());
            builder.append(bad).append("\n");
            for (int i = lineNumber; i < lines.size(); i++) {
                builder.append(lines.get(i)).append("\n");
            }

            builder.style(DEFAULT);
            out.print(builder.toAnsi());
        }
        else {
            String prefix = format("LINE %s: ", lineNumber);
            String padding = " ".repeat(prefix.length() + (columnNumber - 1));
            out.println(prefix + errorLine);
            out.println(padding + "^");
        }
    }

    private static class PrintStreamWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private final PrintStream printStream;

        PrintStreamWarningsPrinter(PrintStream printStream, Theme theme)
        {
            super(OptionalInt.empty(), theme);
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            warnings.forEach(printStream::println);
        }

        @Override
        protected void printSeparator()
        {
            printStream.println();
        }
    }
}

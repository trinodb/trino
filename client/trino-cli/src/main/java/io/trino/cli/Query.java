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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.trino.cli.ClientOptions.OutputFormat;
import io.trino.client.ClientSelectedRole;
import io.trino.client.Column;
import io.trino.client.ErrorLocation;
import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import io.trino.client.Warning;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_HEADER_AND_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.NO_QUOTES;
import static io.trino.cli.CsvPrinter.CsvOutputFormat.STANDARD;
import static io.trino.cli.TerminalUtils.isRealTerminal;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.logging.Level.FINE;
import static org.jline.utils.AttributedStyle.CYAN;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;

public class Query
        implements Closeable
{
    private static final Logger log = Logger.getLogger(Query.class.getName());

    private final AtomicBoolean ignoreUserInterrupt = new AtomicBoolean();
    private final StatementClient client;
    private final boolean debug;

    public Query(StatementClient client, boolean debug)
    {
        this.client = requireNonNull(client, "client is null");
        this.debug = debug;
    }

    public Optional<String> getSetCatalog()
    {
        return client.getSetCatalog();
    }

    public Optional<String> getSetSchema()
    {
        return client.getSetSchema();
    }

    public Optional<String> getSetPath()
    {
        return client.getSetPath();
    }

    public Optional<String> getSetAuthorizationUser()
    {
        return client.getSetAuthorizationUser();
    }

    public boolean isResetAuthorizationUser()
    {
        return client.isResetAuthorizationUser();
    }

    public Map<String, String> getSetSessionProperties()
    {
        return client.getSetSessionProperties();
    }

    public Set<String> getResetSessionProperties()
    {
        return client.getResetSessionProperties();
    }

    public Map<String, ClientSelectedRole> getSetRoles()
    {
        return client.getSetRoles();
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return client.getAddedPreparedStatements();
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return client.getDeallocatedPreparedStatements();
    }

    public String getStartedTransactionId()
    {
        return client.getStartedTransactionId();
    }

    public boolean isClearTransactionId()
    {
        return client.isClearTransactionId();
    }

    public boolean renderOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, OutputFormat outputFormat, Optional<String> pager, boolean showProgress)
    {
        Thread clientThread = Thread.currentThread();
        SignalHandler oldHandler = terminal.handle(Signal.INT, signal -> {
            if (ignoreUserInterrupt.get() || client.isClientAborted()) {
                return;
            }
            client.close();
            clientThread.interrupt();
        });
        try {
            return renderQueryOutput(terminal, out, errorChannel, outputFormat, pager, showProgress);
        }
        finally {
            terminal.handle(Signal.INT, oldHandler);
            Thread.interrupted(); // clear interrupt status
        }
    }

    private boolean renderQueryOutput(Terminal terminal, PrintStream out, PrintStream errorChannel, OutputFormat outputFormat, Optional<String> pager, boolean showProgress)
    {
        StatusPrinter statusPrinter = null;
        WarningsPrinter warningsPrinter = new PrintStreamWarningsPrinter(errorChannel);

        if (showProgress) {
            statusPrinter = new StatusPrinter(client, errorChannel, debug, isInteractive(pager));
            statusPrinter.printInitialStatusUpdates(terminal);
        }
        else {
            processInitialStatusUpdates(warningsPrinter);
        }

        // if running or finished
        if (client.isRunning() || (client.isFinished() && client.finalStatusInfo().getError() == null)) {
            QueryStatusInfo results = client.isRunning() ? client.currentStatusInfo() : client.finalStatusInfo();
            if (results.getUpdateType() != null) {
                renderUpdate(terminal, errorChannel, results, outputFormat, pager);
            }
            // TODO once https://github.com/trinodb/trino/issues/14253 is done this else here should be needed
            // and should be replaced with just simple:
            // if there is updateCount print it
            // if there are columns(resultSet) then print it
            else if (results.getColumns() == null) {
                errorChannel.printf("Query %s has no columns\n", results.getId());
                return false;
            }
            else {
                renderResults(terminal, out, outputFormat, pager, results.getColumns());
            }
        }

        checkState(!client.isRunning());

        warningsPrinter.print(client.finalStatusInfo().getWarnings(), true, true);

        if (showProgress) {
            statusPrinter.printFinalInfo();
        }

        if (client.isClientAborted()) {
            errorChannel.println("Query aborted by user");
            return false;
        }
        if (client.isClientError()) {
            errorChannel.println("Query is gone (server restarted?)");
            return false;
        }

        verify(client.isFinished());
        if (client.finalStatusInfo().getError() != null) {
            renderFailure(errorChannel);
            return false;
        }

        return true;
    }

    private boolean isInteractive(Optional<String> pager)
    {
        return pager.map(name -> name.trim().length() != 0).orElse(true);
    }

    private void processInitialStatusUpdates(WarningsPrinter warningsPrinter)
    {
        while (client.isRunning() && (client.currentData().getData() == null)) {
            warningsPrinter.print(client.currentStatusInfo().getWarnings(), true, false);
            try {
                client.advance();
            }
            catch (RuntimeException e) {
                log.log(FINE, "error printing status", e);
            }
        }
        List<Warning> warnings;
        if (client.isRunning()) {
            warnings = client.currentStatusInfo().getWarnings();
        }
        else {
            warnings = client.finalStatusInfo().getWarnings();
        }
        warningsPrinter.print(warnings, false, true);
    }

    private void renderUpdate(Terminal terminal, PrintStream out, QueryStatusInfo results, OutputFormat outputFormat, Optional<String> pager)
    {
        String status = results.getUpdateType();
        if (results.getUpdateCount() != null) {
            long count = results.getUpdateCount();
            status += format(": %s row%s", count, (count != 1) ? "s" : "");
            out.println(status);
        }
        else if (results.getColumns() != null && !results.getColumns().isEmpty()) {
            out.println(status);
            renderResults(terminal, out, outputFormat, pager, results.getColumns());
        }
        else {
            out.println(status);
        }
        discardResults();
    }

    private void discardResults()
    {
        try (OutputHandler handler = new OutputHandler(new NullPrinter())) {
            handler.processRows(client);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void renderResults(Terminal terminal, PrintStream out, OutputFormat outputFormat, Optional<String> pager, List<Column> columns)
    {
        try {
            doRenderResults(terminal, out, outputFormat, pager, columns);
        }
        catch (QueryAbortedException e) {
            System.out.println("(query aborted by user)");
            client.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
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
                    client.close();
                    clientThread.interrupt();
                });
            }
            handler.processRows(client);
        }
        catch (RuntimeException | IOException e) {
            if (client.isClientAborted() && !(e instanceof QueryAbortedException)) {
                throw new QueryAbortedException(e);
            }
            throw e;
        }
    }

    private void sendOutput(PrintStream out, OutputFormat format, int maxWidth, List<Column> fieldNames)
            throws IOException
    {
        try (OutputHandler handler = createOutputHandler(format, maxWidth, createWriter(out), fieldNames)) {
            handler.processRows(client);
        }
    }

    private static OutputHandler createOutputHandler(OutputFormat format, int maxWidth, Writer writer, List<Column> columns)
    {
        return new OutputHandler(createOutputPrinter(format, maxWidth, writer, columns));
    }

    private static OutputPrinter createOutputPrinter(OutputFormat format, int maxWidth, Writer writer, List<Column> columns)
    {
        List<String> fieldNames = columns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        switch (format) {
            case AUTO:
                return new AutoTablePrinter(columns, writer, maxWidth);
            case ALIGNED:
                return new AlignedTablePrinter(columns, writer);
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
        client.close();
    }

    public void renderFailure(PrintStream out)
    {
        QueryStatusInfo results = client.finalStatusInfo();
        QueryError error = results.getError();
        checkState(error != null);

        out.printf("Query %s failed: %s%n", results.getId(), error.getMessage());
        if (debug && (error.getFailureInfo() != null)) {
            error.getFailureInfo().toException().printStackTrace(out);
        }
        if (error.getErrorLocation() != null) {
            renderErrorLocation(client.getQuery(), error.getErrorLocation(), out);
        }
        out.println();
    }

    private static void renderErrorLocation(String query, ErrorLocation location, PrintStream out)
    {
        List<String> lines = ImmutableList.copyOf(Splitter.on('\n').split(query).iterator());

        String errorLine = lines.get(location.getLineNumber() - 1);
        String good = errorLine.substring(0, location.getColumnNumber() - 1);
        String bad = errorLine.substring(location.getColumnNumber() - 1);

        if ((location.getLineNumber() == lines.size()) && bad.trim().isEmpty()) {
            bad = " <EOF>";
        }

        if (isRealTerminal()) {
            AttributedStringBuilder builder = new AttributedStringBuilder();

            builder.style(DEFAULT.foreground(CYAN));
            for (int i = 1; i < location.getLineNumber(); i++) {
                builder.append(lines.get(i - 1)).append("\n");
            }
            builder.append(good);

            builder.style(DEFAULT.foreground(RED));
            builder.append(bad).append("\n");
            for (int i = location.getLineNumber(); i < lines.size(); i++) {
                builder.append(lines.get(i)).append("\n");
            }

            builder.style(DEFAULT);
            out.print(builder.toAnsi());
        }
        else {
            String prefix = format("LINE %s: ", location.getLineNumber());
            String padding = Strings.repeat(" ", prefix.length() + (location.getColumnNumber() - 1));
            out.println(prefix + errorLine);
            out.println(padding + "^");
        }
    }

    private static class PrintStreamWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private final PrintStream printStream;

        PrintStreamWarningsPrinter(PrintStream printStream)
        {
            super(OptionalInt.empty());
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

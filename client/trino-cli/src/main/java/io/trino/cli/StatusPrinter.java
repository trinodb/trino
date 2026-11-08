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

import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.jdbc.QueryStats;
import io.trino.jdbc.StageStats;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Strings.padStart;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.Duration.nanosSince;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.cli.FormatUtils.formatCount;
import static io.trino.cli.FormatUtils.formatCountRate;
import static io.trino.cli.FormatUtils.formatDataRate;
import static io.trino.cli.FormatUtils.formatDataSize;
import static io.trino.cli.FormatUtils.formatFinalTime;
import static io.trino.cli.FormatUtils.formatProgressBar;
import static io.trino.cli.FormatUtils.formatTime;
import static io.trino.cli.FormatUtils.pluralize;
import static io.trino.cli.TerminalUtils.isRealTerminal;
import static io.trino.cli.TerminalUtils.terminalWidth;
import static java.lang.Character.toUpperCase;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jline.utils.AttributedStyle.DEFAULT;

public class StatusPrinter
{
    private static final int CTRL_C = 3;
    private static final int CTRL_P = 16;
    private static final long POLL_INTERVAL_MILLIS = 50;

    private final long start = System.nanoTime();
    private final StatementExecution execution;
    private final Runnable cancelQuery;
    private final PrintStream out;
    private final ConsolePrinter console;
    private final boolean checkInput;
    private final boolean decimalDataSize;
    private final Theme theme;

    private boolean debug;

    public StatusPrinter(StatementExecution execution, Runnable cancelQuery, PrintStream out, boolean debug, boolean checkInput, boolean decimalDataSize, Theme theme)
    {
        this.execution = requireNonNull(execution, "execution is null");
        this.cancelQuery = requireNonNull(cancelQuery, "cancelQuery is null");
        this.out = out;
        this.console = new ConsolePrinter(out);
        this.debug = debug;
        this.checkInput = checkInput;
        this.decimalDataSize = decimalDataSize;
        this.theme = requireNonNull(theme, "theme is null");
    }

/*

Query 16, RUNNING, 1 node, 855 splits
http://my.server:8080/v1/query/16?pretty
Splits:   646 queued, 34 running, 175 done
CPU Time: 33.7s total,  191K rows/s, 16.6MB/s, 22% active
Per Node: 2.5 parallelism,  473K rows/s, 41.1MB/s
Parallelism: 2.5
Peak Memory: 1.97GB
Spilled: 20GB
0:13 [6.45M rows,  560MB] [ 473K rows/s, 41.1MB/s] [=========>>           ] 20%

     STAGES   ROWS  ROWS/s  BYTES  BYTES/s   PEND    RUN   DONE
0.........R  13.8M    336K  1.99G    49.5M      0      1    706
  1.......R   666K   41.5K  82.1M    5.12M    563     65     79
    2.....R  4.58M    234K   620M    31.6M    406     65    236

 */

    public void printInitialStatusUpdates(Terminal terminal)
            throws InterruptedException
    {
        Attributes originalAttributes = terminal.enterRawMode();
        long lastPrint = System.nanoTime();
        try {
            WarningsPrinter warningsPrinter = new ConsoleWarningsPrinter(console, theme);
            // exit status loop when there is pending output or the query finished
            while (!execution.awaitResults(POLL_INTERVAL_MILLIS, MILLISECONDS)) {
                try {
                    // check if time to update screen
                    boolean update = nanosSince(lastPrint).getValue(SECONDS) >= 0.5;

                    // start checking for input after 300ms to avoid delaying short queries
                    if (checkInput && nanosSince(start).getValue(SECONDS) >= 0.3) {
                        // check for keyboard input
                        int key = readKey(terminal);
                        if (key == CTRL_P) {
                            execution.partialCancel();
                        }
                        else if (key == CTRL_C) {
                            updateScreen(terminal, warningsPrinter);
                            update = false;
                            cancelQuery.run();
                        }
                        else if (toUpperCase(key) == 'D') {
                            debug = !debug;
                            console.resetScreen();
                            update = true;
                        }
                    }

                    // update screen
                    if (update) {
                        updateScreen(terminal, warningsPrinter);
                        lastPrint = System.nanoTime();
                    }
                }
                catch (RuntimeException e) {
                    if (debug) {
                        e.printStackTrace(out);
                    }
                }
            }
        }
        finally {
            console.resetScreen();
            if (checkInput) {
                discardKeys(terminal);
            }
            terminal.setAttributes(originalAttributes);
        }
    }

    private void updateScreen(Terminal terminal, WarningsPrinter warningsPrinter)
    {
        // The redraw goes to out and the sequences to the terminal writer, so flush both in order.
        terminal.beginSynchronizedUpdate();
        terminal.flush();
        try {
            redrawScreen(warningsPrinter);
        }
        finally {
            out.flush();
            terminal.endSynchronizedUpdate();
            terminal.flush();
        }
    }

    private void redrawScreen(WarningsPrinter warningsPrinter)
    {
        console.repositionCursor();
        Optional<QueryStats> stats = execution.getStats();
        if (stats.isPresent()) {
            printQueryInfo(stats.get(), warningsPrinter);
        }
        else {
            printGenericQueryInfo(warningsPrinter);
        }
    }

    public void printFinalInfo()
    {
        Optional<QueryStats> finalStats = execution.getStats();
        if (finalStats.isEmpty()) {
            return;
        }
        QueryStats stats = finalStats.get();

        Duration wallTime = succinctDuration(stats.getElapsedTimeMillis(), MILLISECONDS);

        int nodes = stats.getNodes();
        if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
            return;
        }

        // blank line
        out.println();

        // Query 12, FINISHED, 1 node
        String querySummary = format(
                "Query %s, %s, %,d %s",
                stats.getQueryId(),
                coloredState(stats.getState()),
                nodes,
                pluralize("node", nodes));
        out.println(querySummary);

        if (debug) {
            stats.getInfoUri().ifPresent(out::println);
        }

        // Splits: 1000 total, 842 done (84.20%)
        String splitsSummary = format(
                "Splits: %s total, %s done (%.2f%%)",
                coloredCount(stats.getTotalSplits()),
                coloredCount(stats.getCompletedSplits()),
                stats.getProgressPercentage().orElse(0.0));
        out.println(splitsSummary);

        if (debug) {
            // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
            Duration cpuTime = millis(stats.getCpuTimeMillis());
            String cpuTimeSummary = format(
                    "CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                    cpuTime.getValue(SECONDS),
                    formatCountRate(stats.getProcessedRows(), cpuTime, false),
                    formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true, decimalDataSize),
                    (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
            out.println(cpuTimeSummary);

            double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

            // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
            String perNodeSummary = format(
                    "Per Node: %.1f parallelism, %5s rows/s, %8s",
                    parallelism / nodes,
                    formatCountRate((double) stats.getProcessedRows() / nodes, wallTime, false),
                    formatDataRate(bytes(stats.getProcessedBytes() / nodes), wallTime, true, decimalDataSize));
            reprintLine(perNodeSummary);

            // Parallelism: 5.3
            out.printf("Parallelism: %.1f%n", parallelism);

            // Peak Memory: 1.97GB
            reprintLine("Peak Memory: " + formatDataSize(bytes(stats.getPeakMemoryBytes()), true, decimalDataSize));

            // Spilled Data: 20GB
            if (stats.getSpilledBytes() > 0) {
                reprintLine("Spilled: " + formatDataSize(bytes(stats.getSpilledBytes()), true, decimalDataSize));
            }
        }

        // 0:32 [2.12GB, 15M rows] [67MB/s, 463K rows/s]
        String statsLine = format(
                "%s [%s rows, %s] [%s rows/s, %s]",
                formatFinalTime(wallTime),
                coloredNumber(formatCount(stats.getProcessedRows())),
                coloredNumber(formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize)),
                coloredNumber(formatCountRate(stats.getProcessedRows(), wallTime, false)),
                coloredNumber(formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize)));

        out.println(statsLine);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryStats stats, WarningsPrinter warningsPrinter)
    {
        Duration wallTime = nanosSince(start);

        // cap progress at 99%, otherwise it looks weird when the query is still running and it says 100%
        int progressPercentage = (int) min(99, stats.getProgressPercentage().orElse(0.0));

        if (isRealTerminal()) {
            // blank line
            reprintLine("");

            int terminalWidth = terminalWidth();

            if (terminalWidth < 75) {
                reprintLine("WARNING: Terminal");
                reprintLine("must be at least");
                reprintLine("80 characters wide");
                reprintLine("");
                reprintLine(coloredState(stats.getState()));
                reprintLine(format("%s %d%%", formatTime(wallTime), progressPercentage));
                return;
            }

            int nodes = stats.getNodes();

            // Query 10, RUNNING, 1 node, 778 splits
            String querySummary = format(
                    "Query %s, %s, %,d %s, %s splits",
                    stats.getQueryId(),
                    coloredState(stats.getState()),
                    nodes,
                    pluralize("node", nodes),
                    coloredCount(stats.getTotalSplits()));
            reprintLine(querySummary);

            Optional<String> url = stats.getInfoUri().map(Object::toString);
            if (debug && url.isPresent() && (url.get().length() < terminalWidth)) {
                reprintLine(url.get());
            }

            if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
                return;
            }

            if (debug) {
                // Splits:   620 queued, 34 running, 124 done
                String splitsSummary = format(
                        "Splits:   %,d queued, %,d running, %,d done",
                        stats.getQueuedSplits(),
                        stats.getRunningSplits(),
                        stats.getCompletedSplits());
                reprintLine(splitsSummary);

                // CPU Time: 56.5s total, 36.4K rows/s, 4.44MB/s, 60% active
                Duration cpuTime = millis(stats.getCpuTimeMillis());
                String cpuTimeSummary = format(
                        "CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                        cpuTime.getValue(SECONDS),
                        formatCountRate(stats.getProcessedRows(), cpuTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true, decimalDataSize),
                        (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
                reprintLine(cpuTimeSummary);

                double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

                // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
                String perNodeSummary = format(
                        "Per Node: %.1f parallelism, %5s rows/s, %8s",
                        parallelism / nodes,
                        formatCountRate((double) stats.getProcessedRows() / nodes, wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes() / nodes), wallTime, true, decimalDataSize));
                reprintLine(perNodeSummary);

                // Parallelism: 5.3
                reprintLine(format("Parallelism: %.1f", parallelism));

                // Peak Memory: 1.97GB
                reprintLine("Peak Memory: " + formatDataSize(bytes(stats.getPeakMemoryBytes()), true, decimalDataSize));

                // Spilled Data: 20GB
                if (stats.getSpilledBytes() > 0) {
                    reprintLine("Spilled: " + formatDataSize(bytes(stats.getSpilledBytes()), true, decimalDataSize));
                }
            }

            verify(terminalWidth >= 75); // otherwise handled above
            int progressWidth = (min(terminalWidth, 100) - 75) + 17; // progress bar is 17-42 characters wide

            if (stats.isScheduled()) {
                String progressBar = formatProgressBar(
                        progressWidth,
                        progressPercentage,
                        (int) ceil(stats.getRunningPercentage().orElse(0.0)));

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [=====>>                                   ] 10%
                String progressLine = format(
                        "%s [%s rows, %s] [%s rows/s, %s] [%s] %d%%",
                        formatTime(wallTime),
                        coloredNumber(formatCount(stats.getProcessedRows()), 5),
                        coloredNumber(formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize), 6),
                        coloredNumber(formatCountRate(stats.getProcessedRows(), wallTime, false), 5),
                        coloredNumber(formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize), 8),
                        progressBar,
                        progressPercentage);

                reprintLine(progressLine);
            }
            else {
                String progressBar = formatProgressBar(progressWidth, Ints.saturatedCast(nanosSince(start).roundTo(SECONDS)));

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [    <=>                                  ]
                String progressLine = format(
                        "%s [%s rows, %s] [%s rows/s, %s] [%s]",
                        formatTime(wallTime),
                        coloredNumber(formatCount(stats.getProcessedRows()), 5),
                        coloredNumber(formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize), 6),
                        coloredNumber(formatCountRate(stats.getProcessedRows(), wallTime, false), 5),
                        coloredNumber(formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize), 8),
                        progressBar);

                reprintLine(progressLine);
            }

            // blank line
            reprintLine("");

            // STAGE  S    ROWS    RPS  BYTES    BPS   QUEUED    RUN   DONE
            String stagesHeader = format(
                    "%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                    "STAGE",
                    "S",
                    "ROWS",
                    "ROWS/s",
                    "BYTES",
                    "BYTES/s",
                    "QUEUED",
                    "RUN",
                    "DONE");
            reprintLine(colored(stagesHeader, theme.keyword()));

            stats.getRootStage().ifPresent(stage -> printStageTree(stage, ""));
        }
        else {
            // Query 31 [S] i[2.7M 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = format(
                    "Query %s [%s] i[%s %s %s] o[%s %s %s] splits[%,d/%,d/%,d]",
                    stats.getQueryId(),
                    coloredState(stats.getState()),

                    formatCount(stats.getProcessedRows()),
                    formatDataSize(bytes(stats.getProcessedBytes()), false, decimalDataSize),
                    formatDataRate(bytes(stats.getProcessedBytes()), wallTime, false, decimalDataSize),

                    formatCount(stats.getProcessedRows()),
                    formatDataSize(bytes(stats.getProcessedBytes()), false, decimalDataSize),
                    formatDataRate(bytes(stats.getProcessedBytes()), wallTime, false, decimalDataSize),

                    stats.getQueuedSplits(),
                    stats.getRunningSplits(),
                    stats.getCompletedSplits());
            reprintLine(querySummary);
        }
        warningsPrinter.print(execution.getWarnings(), true, false);
    }

    // Used for drivers that do not report progress
    private void printGenericQueryInfo(WarningsPrinter warningsPrinter)
    {
        Duration wallTime = nanosSince(start);
        if (!isRealTerminal()) {
            reprintLine(format("Query [%s] %s", coloredState("RUNNING"), formatTime(wallTime)));
            return;
        }

        // blank line
        reprintLine("");

        int terminalWidth = terminalWidth();
        if (terminalWidth < 75) {
            reprintLine(coloredState("RUNNING"));
            reprintLine(formatTime(wallTime));
            return;
        }

        int progressWidth = (min(terminalWidth, 100) - 75) + 17;
        String progressBar = formatProgressBar(progressWidth, Ints.saturatedCast(wallTime.roundTo(SECONDS)));

        // RUNNING 0:17 [    <=>                                  ]
        reprintLine(format("%s %s [%s]", coloredState("RUNNING"), formatTime(wallTime), progressBar));

        // blank line
        reprintLine("");
        warningsPrinter.print(execution.getWarnings(), true, false);
    }

    private void printStageTree(StageStats stage, String indent)
    {
        Duration elapsedTime = nanosSince(start);

        // STAGE  S    ROWS  ROWS/s  BYTES  BYTES/s  QUEUED    RUN   DONE
        // 0......Q     26M   9077M  9993G    9077M   9077M  9077M  9077M
        //   2....R     17K    627M   673M     627M    627M   627M   627M
        //     3..C     999    627M   673M     627M    627M   627M   627M
        //   4....R     26M    627M   673T     627M    627M   627M   627M
        //     5..F     29T    627M   673M     627M    627M   627M   627M

        String id = stage.getStageId();
        String name = indent + id;
        name += ".".repeat(max(0, 10 - name.length()));

        String bytesPerSecond;
        String rowsPerSecond;
        if (stage.isDone()) {
            bytesPerSecond = formatDataRate(DataSize.ofBytes(0), new Duration(0, SECONDS), false, decimalDataSize);
            rowsPerSecond = formatCountRate(0, new Duration(0, SECONDS), false);
        }
        else {
            bytesPerSecond = formatDataRate(bytes(stage.getProcessedBytes()), elapsedTime, false, decimalDataSize);
            rowsPerSecond = formatCountRate(stage.getProcessedRows(), elapsedTime, false);
        }

        String stageSummary = format(
                "%10s%s  %s  %s  %s  %s  %s  %s  %s",
                name,
                coloredStateCharacter(stage.getState()),

                coloredNumber(formatCount(stage.getProcessedRows()), 5),
                coloredNumber(rowsPerSecond, 6),

                coloredNumber(formatDataSize(bytes(stage.getProcessedBytes()), false, decimalDataSize), 5),
                coloredNumber(bytesPerSecond, 7),

                coloredNumber(String.valueOf(stage.getQueuedSplits()), 6),
                coloredNumber(String.valueOf(stage.getRunningSplits()), 5),
                coloredNumber(String.valueOf(stage.getCompletedSplits()), 5));
        reprintLine(stageSummary);

        for (StageStats subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ");
        }
    }

    private void reprintLine(String line)
    {
        console.reprintLine(line);
    }

    private static int readKey(Terminal terminal)
    {
        try {
            return terminal.reader().read(1L);
        }
        catch (IOException e) {
            return -1;
        }
    }

    private static void discardKeys(Terminal terminal)
    {
        while (readKey(terminal) >= 0) {
            // discard input
        }
    }

    private static char stageStateCharacter(String state)
    {
        return "FAILED".equals(state) ? 'X' : state.charAt(0);
    }

    private String coloredState(String state)
    {
        return colored(state, stateStyle(state));
    }

    private String coloredStateCharacter(String state)
    {
        return colored(String.valueOf(stageStateCharacter(state)), stateStyle(state));
    }

    private AttributedStyle stateStyle(String state)
    {
        if ("FINISHED".equals(state)) {
            return theme.string();
        }
        if ("FAILED".equals(state)) {
            return theme.error();
        }
        return theme.warning();
    }

    private String coloredCount(long value)
    {
        return colored(format("%,d", value), theme.number());
    }

    private String coloredNumber(String value)
    {
        return colored(value, theme.number());
    }

    private String coloredNumber(String value, int width)
    {
        return colored(padStart(value, width, ' '), theme.number());
    }

    private String colored(String value, AttributedStyle style)
    {
        if ((style == DEFAULT) || value.isEmpty()) {
            return value;
        }
        return new AttributedString(value, style).toAnsi();
    }

    private static Duration millis(long millis)
    {
        return new Duration(millis, MILLISECONDS);
    }

    private static DataSize bytes(long bytes)
    {
        return DataSize.ofBytes(bytes);
    }

    private static double percentage(double count, double total)
    {
        if (total == 0) {
            return 0;
        }
        return min(100, (count * 100.0) / total);
    }

    private static class ConsoleWarningsPrinter
            extends AbstractWarningsPrinter
    {
        private static final int DISPLAYED_WARNINGS = 5;
        private final ConsolePrinter console;

        ConsoleWarningsPrinter(ConsolePrinter console, Theme theme)
        {
            super(OptionalInt.of(DISPLAYED_WARNINGS), theme);
            this.console = requireNonNull(console, "console is null");
        }

        @Override
        protected void print(List<String> warnings)
        {
            for (String warning : warnings) {
                console.reprintLine(warning);
            }
        }

        @Override
        protected void printSeparator()
        {
            console.reprintLine("");
        }
    }
}

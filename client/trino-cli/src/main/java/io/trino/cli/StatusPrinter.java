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

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StageStats;
import io.trino.client.StatementClient;
import io.trino.client.StatementStats;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

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

public class StatusPrinter
{
    private static final int CTRL_C = 3;
    private static final int CTRL_P = 16;

    private final long start = System.nanoTime();
    private final StatementClient client;
    private final PrintStream out;
    private final ConsolePrinter console;
    private final boolean checkInput;
    private final boolean decimalDataSize;

    private boolean debug;

    public StatusPrinter(StatementClient client, PrintStream out, boolean debug, boolean checkInput, boolean decimalDataSize)
    {
        this.client = client;
        this.out = out;
        this.console = new ConsolePrinter(out);
        this.debug = debug;
        this.checkInput = checkInput;
        this.decimalDataSize = decimalDataSize;
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
    {
        Attributes originalAttributes = terminal.enterRawMode();
        long start = System.nanoTime();
        long lastPrint = System.nanoTime();
        try {
            WarningsPrinter warningsPrinter = new ConsoleWarningsPrinter(console);
            while (client.isRunning()) {
                try {
                    // exit status loop if there is pending output
                    if (!client.currentRows().isNull()) {
                        return;
                    }

                    // check if time to update screen
                    boolean update = nanosSince(lastPrint).getValue(SECONDS) >= 0.5;

                    // start checking for input after 300ms to avoid delaying short queries
                    if (checkInput && nanosSince(start).getValue(SECONDS) >= 0.3) {
                        // check for keyboard input
                        int key = readKey(terminal);
                        if (key == CTRL_P) {
                            client.cancelLeafStage();
                        }
                        else if (key == CTRL_C) {
                            updateScreen(warningsPrinter);
                            update = false;
                            client.close();
                        }
                        else if (toUpperCase(key) == 'D') {
                            debug = !debug;
                            console.resetScreen();
                            update = true;
                        }
                    }

                    // update screen
                    if (update) {
                        updateScreen(warningsPrinter);
                        lastPrint = System.nanoTime();
                    }

                    // fetch next results (server will wait for a while if no data)
                    client.advance();
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

    private void updateScreen(WarningsPrinter warningsPrinter)
    {
        console.repositionCursor();
        printQueryInfo(client.currentStatusInfo(), warningsPrinter);
    }

    public void printFinalInfo()
    {
        QueryStatusInfo results = client.finalStatusInfo();
        StatementStats stats = results.getStats();

        Duration wallTime = succinctDuration(stats.getElapsedTimeMillis(), MILLISECONDS);

        int nodes = stats.getNodes();
        if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
            return;
        }

        // blank line
        out.println();

        // Query 12, FINISHED, 1 node
        String querySummary = format("Query %s, %s, %,d %s",
                results.getId(),
                stats.getState(),
                nodes,
                pluralize("node", nodes));
        out.println(querySummary);

        if (debug) {
            out.println(results.getInfoUri().toString());
        }

        // Splits: 1000 total, 842 done (84.20%)
        String splitsSummary = format("Splits: %,d total, %,d done (%.2f%%)",
                stats.getTotalSplits(),
                stats.getCompletedSplits(),
                stats.getProgressPercentage().orElse(0.0));
        out.println(splitsSummary);

        if (debug) {
            // CPU Time: 565.2s total,   26K rows/s, 3.85MB/s
            Duration cpuTime = millis(stats.getCpuTimeMillis());
            String cpuTimeSummary = format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                    cpuTime.getValue(SECONDS),
                    formatCountRate(stats.getProcessedRows(), cpuTime, false),
                    formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true, decimalDataSize),
                    (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
            out.println(cpuTimeSummary);

            double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

            // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
            String perNodeSummary = format("Per Node: %.1f parallelism, %5s rows/s, %8s",
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
        String statsLine = format("%s [%s rows, %s] [%s rows/s, %s]",
                formatFinalTime(wallTime),
                formatCount(stats.getProcessedRows()),
                formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize),
                formatCountRate(stats.getProcessedRows(), wallTime, false),
                formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize));

        out.println(statsLine);

        // blank line
        out.println();
    }

    private void printQueryInfo(QueryStatusInfo results, WarningsPrinter warningsPrinter)
    {
        StatementStats stats = results.getStats();
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
                reprintLine(stats.getState());
                reprintLine(format("%s %d%%", formatTime(wallTime), progressPercentage));
                return;
            }

            int nodes = stats.getNodes();

            // Query 10, RUNNING, 1 node, 778 splits
            String querySummary = format("Query %s, %s, %,d %s, %,d splits",
                    results.getId(),
                    stats.getState(),
                    nodes,
                    pluralize("node", nodes),
                    stats.getTotalSplits());
            reprintLine(querySummary);

            String url = results.getInfoUri().toString();
            if (debug && (url.length() < terminalWidth)) {
                reprintLine(url);
            }

            if ((nodes == 0) || (stats.getTotalSplits() == 0)) {
                return;
            }

            if (debug) {
                // Splits:   620 queued, 34 running, 124 done
                String splitsSummary = format("Splits:   %,d queued, %,d running, %,d done",
                        stats.getQueuedSplits(),
                        stats.getRunningSplits(),
                        stats.getCompletedSplits());
                reprintLine(splitsSummary);

                // CPU Time: 56.5s total, 36.4K rows/s, 4.44MB/s, 60% active
                Duration cpuTime = millis(stats.getCpuTimeMillis());
                String cpuTimeSummary = format("CPU Time: %.1fs total, %5s rows/s, %8s, %d%% active",
                        cpuTime.getValue(SECONDS),
                        formatCountRate(stats.getProcessedRows(), cpuTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), cpuTime, true, decimalDataSize),
                        (int) percentage(stats.getCpuTimeMillis(), stats.getWallTimeMillis()));
                reprintLine(cpuTimeSummary);

                double parallelism = cpuTime.getValue(MILLISECONDS) / wallTime.getValue(MILLISECONDS);

                // Per Node: 3.5 parallelism, 83.3K rows/s, 0.7 MB/s
                String perNodeSummary = format("Per Node: %.1f parallelism, %5s rows/s, %8s",
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
                String progressLine = format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s] %d%%",
                        formatTime(wallTime),
                        formatCount(stats.getProcessedRows()),
                        formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize),
                        formatCountRate(stats.getProcessedRows(), wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize),
                        progressBar,
                        progressPercentage);

                reprintLine(progressLine);
            }
            else {
                String progressBar = formatProgressBar(progressWidth, Ints.saturatedCast(nanosSince(start).roundTo(SECONDS)));

                // 0:17 [ 103MB,  802K rows] [5.74MB/s, 44.9K rows/s] [    <=>                                  ]
                String progressLine = format("%s [%5s rows, %6s] [%5s rows/s, %8s] [%s]",
                        formatTime(wallTime),
                        formatCount(stats.getProcessedRows()),
                        formatDataSize(bytes(stats.getProcessedBytes()), true, decimalDataSize),
                        formatCountRate(stats.getProcessedRows(), wallTime, false),
                        formatDataRate(bytes(stats.getProcessedBytes()), wallTime, true, decimalDataSize),
                        progressBar);

                reprintLine(progressLine);
            }

            // blank line
            reprintLine("");

            // STAGE  S    ROWS    RPS  BYTES    BPS   QUEUED    RUN   DONE
            String stagesHeader = format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                    "STAGE",
                    "S",
                    "ROWS",
                    "ROWS/s",
                    "BYTES",
                    "BYTES/s",
                    "QUEUED",
                    "RUN",
                    "DONE");
            reprintLine(stagesHeader);

            printStageTree(stats.getRootStage(), "", new AtomicInteger());
        }
        else {
            // Query 31 [S] i[2.7M 67.3MB 62.7MBps] o[35 6.1KB 1KBps] splits[252/16/380]
            String querySummary = format("Query %s [%s] i[%s %s %s] o[%s %s %s] splits[%,d/%,d/%,d]",
                    results.getId(),
                    stats.getState(),

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
        warningsPrinter.print(results.getWarnings(), true, false);
    }

    private void printStageTree(StageStats stage, String indent, AtomicInteger stageNumberCounter)
    {
        Duration elapsedTime = nanosSince(start);

        // STAGE  S    ROWS  ROWS/s  BYTES  BYTES/s  QUEUED    RUN   DONE
        // 0......Q     26M   9077M  9993G    9077M   9077M  9077M  9077M
        //   2....R     17K    627M   673M     627M    627M   627M   627M
        //     3..C     999    627M   673M     627M    627M   627M   627M
        //   4....R     26M    627M   673T     627M    627M   627M   627M
        //     5..F     29T    627M   673M     627M    627M   627M   627M

        String id = String.valueOf(stageNumberCounter.getAndIncrement());
        String name = indent + id;
        name += Strings.repeat(".", max(0, 10 - name.length()));

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

        String stageSummary = format("%10s%1s  %5s  %6s  %5s  %7s  %6s  %5s  %5s",
                name,
                stageStateCharacter(stage.getState()),

                formatCount(stage.getProcessedRows()),
                rowsPerSecond,

                formatDataSize(bytes(stage.getProcessedBytes()), false, decimalDataSize),
                bytesPerSecond,

                stage.getQueuedSplits(),
                stage.getRunningSplits(),
                stage.getCompletedSplits());
        reprintLine(stageSummary);

        for (StageStats subStage : stage.getSubStages()) {
            printStageTree(subStage, indent + "  ", stageNumberCounter);
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

        ConsoleWarningsPrinter(ConsolePrinter console)
        {
            super(OptionalInt.of(DISPLAYED_WARNINGS));
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

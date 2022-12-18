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
package io.trino.sql.planner.planprinter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class TextRenderer
        implements Renderer<String>
{
    private final boolean verbose;
    private final int level;

    public TextRenderer(boolean verbose, int level)
    {
        this.verbose = verbose;
        this.level = level;
    }

    @Override
    public String render(PlanRepresentation plan)
    {
        StringBuilder output = new StringBuilder();
        NodeRepresentation root = plan.getRoot();
        boolean hasChildren = hasChildren(root, plan);
        return writeTextOutput(output, plan, Indent.newInstance(level, hasChildren), root);
    }

    private String writeTextOutput(StringBuilder output, PlanRepresentation plan, Indent indent, NodeRepresentation node)
    {
        output.append(indent.nodeIndent())
                .append(node.getName())
                .append(node.getDescriptor().entrySet().stream()
                        .filter(entry -> !(entry.getValue().isEmpty() || entry.getValue().equals("[]")))
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(joining(", ", "[", "]")))
                .append("\n");

        String columns = node.getOutputs().stream()
                .map(s -> s.getSymbol() + ":" + s.getType())
                .collect(joining(", "));

        output.append(indentMultilineString("Layout: [" + columns + "]\n", indent.detailIndent()));

        String reorderJoinStatsAndCost = printReorderJoinStatsAndCost(node);
        if (!reorderJoinStatsAndCost.isEmpty()) {
            output.append(indentMultilineString(reorderJoinStatsAndCost, indent.detailIndent()));
        }

        List<PlanNodeStatsAndCostSummary> estimates = node.getEstimates(plan.getTypes());
        if (!estimates.isEmpty()) {
            output.append(indentMultilineString(printEstimates(estimates), indent.detailIndent()));
        }

        String stats = printStats(plan, node);
        if (!stats.isEmpty()) {
            output.append(indentMultilineString(stats, indent.detailIndent()));
        }

        if (!node.getDetails().isEmpty()) {
            String details = indentMultilineString(Joiner.on("\n").join(node.getDetails()), indent.detailIndent());
            output.append(details);
            if (!details.endsWith("\n")) {
                output.append('\n');
            }
        }

        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        for (Iterator<NodeRepresentation> iterator = children.iterator(); iterator.hasNext(); ) {
            NodeRepresentation child = iterator.next();
            writeTextOutput(output, plan, indent.forChild(!iterator.hasNext(), hasChildren(child, plan)), child);
        }

        return output.toString();
    }

    private String printStats(PlanRepresentation plan, NodeRepresentation node)
    {
        StringBuilder output = new StringBuilder();
        if (node.getStats().isEmpty() || !(plan.getTotalCpuTime().isPresent() && plan.getTotalScheduledTime().isPresent() && plan.getTotalBlockedTime().isPresent())) {
            return "";
        }

        PlanNodeStats nodeStats = node.getStats().get();

        double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / plan.getTotalScheduledTime().get().toMillis();
        double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / plan.getTotalCpuTime().get().toMillis();
        double blockedTimeFraction = 100.0d * nodeStats.getPlanNodeBlockedTime().toMillis() / plan.getTotalBlockedTime().get().toMillis();

        output.append(format("CPU: %s (%s%%), Scheduled: %s (%s%%), Blocked: %s (%s%%)",
                nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                formatDouble(cpuTimeFraction),
                nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                formatDouble(scheduledTimeFraction),
                nodeStats.getPlanNodeBlockedTime().convertToMostSuccinctTimeUnit(),
                formatDouble(blockedTimeFraction)));

        output.append(format(", Output: %s (%s)", formatPositions(nodeStats.getPlanNodeOutputPositions()), nodeStats.getPlanNodeOutputDataSize().toString()));
        if (nodeStats.getPlanNodeSpilledDataSize().toBytes() > 0) {
            output.append(format(", Spilled: %s", nodeStats.getPlanNodeSpilledDataSize()));
        }
        output.append("\n");

        printMetrics(output, "connector metrics:", BasicOperatorStats::getConnectorMetrics, nodeStats);
        printMetrics(output, "metrics:", BasicOperatorStats::getMetrics, nodeStats);
        printDistributions(output, nodeStats);

        if (nodeStats instanceof WindowPlanNodeStats) {
            printWindowOperatorStats(output, ((WindowPlanNodeStats) nodeStats).getWindowOperatorStats());
        }

        return output.toString();
    }

    private void printMetrics(StringBuilder output, String label, Function<BasicOperatorStats, Metrics> metricsGetter, PlanNodeStats stats)
    {
        if (!verbose) {
            return;
        }

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(stats.getOperatorTypes());
        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            Map<String, Metric<?>> metrics = metricsGetter.apply(stats.getOperatorStats().get(operator)).getMetrics();

            // filter out empty distributions
            metrics = metrics.entrySet().stream()
                    .filter(entry -> {
                        if (!(entry.getValue() instanceof TDigestHistogram histogram)) {
                            return true;
                        }

                        return histogram.getMin() != 0. || histogram.getMax() != 0.;
                    })
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            if (metrics.isEmpty()) {
                continue;
            }

            output.append(translatedOperatorType + label).append("\n");
            Map<String, Metric<?>> sortedMap = new TreeMap<>(metrics);
            sortedMap.forEach((name, metric) -> output.append(format("  '%s' = %s\n", name, metric)));
        }
    }

    private void printDistributions(StringBuilder output, PlanNodeStats stats)
    {
        Map<String, Double> inputAverages = stats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = stats.getOperatorInputPositionsStdDevs();
        Map<String, String> translatedOperatorTypes = translateOperatorTypes(stats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            output.append(translatedOperatorType);
            output.append(format(
                    Locale.US,
                    "Input avg.: %s rows, Input std.dev.: %s%%\n",
                    formatDouble(inputAverage),
                    formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));
        }
    }

    private void printWindowOperatorStats(StringBuilder output, WindowOperatorStats stats)
    {
        if (!verbose) {
            // these stats are too detailed for non-verbose mode
            return;
        }

        output.append(format("Active Drivers: [ %d / %d ]\n", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append(format("Index size: std.dev.: %s bytes, %s rows\n", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev())));
        output.append(format("Index count per driver: std.dev.: %s\n", formatDouble(stats.getIndexCountPerDriverStdDev())));
        output.append(format("Rows per driver: std.dev.: %s\n", formatDouble(stats.getRowsPerDriverStdDev())));
        output.append(format("Size of partition: std.dev.: %s\n", formatDouble(stats.getPartitionRowsStdDev())));
    }

    private static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }

    private String printReorderJoinStatsAndCost(NodeRepresentation node)
    {
        if (verbose && node.getReorderJoinStatsAndCost().isPresent()) {
            return format("Reorder joins cost : %s\n", formatPlanNodeStatsAndCostSummary(node.getReorderJoinStatsAndCost().get()));
        }
        return "";
    }

    private String printEstimates(List<PlanNodeStatsAndCostSummary> estimates)
    {
        return estimates.stream()
                .map(this::formatPlanNodeStatsAndCostSummary)
                .collect(joining("/", "Estimates: ", "\n"));
    }

    private String formatPlanNodeStatsAndCostSummary(PlanNodeStatsAndCostSummary stats)
    {
        requireNonNull(stats, "stats is null");
        return format("{rows: %s (%s), cpu: %s, memory: %s, network: %s}",
                formatAsLong(stats.getOutputRowCount()),
                formatAsDataSize(stats.getOutputSizeInBytes()),
                formatAsCpuCost(stats.getCpuCost()),
                formatAsDataSize(stats.getMemoryCost()),
                formatAsDataSize(stats.getNetworkCost()));
    }

    private static boolean hasChildren(NodeRepresentation node, PlanRepresentation plan)
    {
        return node.getChildren().stream()
                .map(plan::getNode)
                .anyMatch(Optional::isPresent);
    }

    private static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%d", Math.round(value));
        }

        return "?";
    }

    private static String formatAsCpuCost(double value)
    {
        return formatAsDataSize(value).replaceAll("B$", "");
    }

    private static String formatAsDataSize(double value)
    {
        if (isNaN(value)) {
            return "?";
        }
        if (value == POSITIVE_INFINITY) {
            return "+\u221E";
        }
        if (value == NEGATIVE_INFINITY) {
            return "-\u221E";
        }

        return DataSize.succinctBytes(Math.round(value)).toString();
    }

    static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "?";
    }

    static String formatPositions(long positions)
    {
        String noun = (positions == 1) ? "row" : "rows";
        return positions + " " + noun;
    }

    static String indentString(int indent)
    {
        return "    ".repeat(indent);
    }

    private static String indentMultilineString(String string, String indent)
    {
        return string.replaceAll("(?m)^", indent);
    }

    private static class Indent
    {
        private static final String VERTICAL_LINE = "\u2502";
        private static final String LAST_NODE = "\u2514\u2500";
        private static final String INTERMEDIATE_NODE = "\u251c\u2500";

        private final String firstLinePrefix;
        private final String nextLinesPrefix;
        private final boolean hasChildren;

        public static Indent newInstance(int level, boolean hasChildren)
        {
            String indent = indentString(level);
            return new Indent(indent, indent, hasChildren);
        }

        private Indent(String firstLinePrefix, String nextLinesPrefix, boolean hasChildren)
        {
            this.firstLinePrefix = firstLinePrefix;
            this.nextLinesPrefix = nextLinesPrefix;
            this.hasChildren = hasChildren;
        }

        public Indent forChild(boolean last, boolean hasChildren)
        {
            String first;
            String next;

            if (last) {
                first = pad(LAST_NODE, 3);
                next = pad("", 3);
            }
            else {
                first = pad(INTERMEDIATE_NODE, 3);
                next = pad(VERTICAL_LINE, 3);
            }

            return new Indent(nextLinesPrefix + first, nextLinesPrefix + next, hasChildren);
        }

        public String nodeIndent()
        {
            return firstLinePrefix;
        }

        public String detailIndent()
        {
            String indent = "";

            if (hasChildren) {
                indent = VERTICAL_LINE;
            }

            return nextLinesPrefix + pad(indent, 4);
        }

        private static String pad(String text, int length)
        {
            checkArgument(text.length() <= length, "text is longer that length");

            return text + " ".repeat(length - text.length());
        }
    }
}

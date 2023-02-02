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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TableWriterNode.CreateReference;
import io.trino.sql.planner.plan.TableWriterNode.CreateTarget;
import io.trino.sql.planner.plan.TableWriterNode.InsertReference;
import io.trino.sql.planner.plan.TableWriterNode.InsertTarget;
import io.trino.sql.planner.plan.TableWriterNode.MergeTarget;
import io.trino.sql.planner.plan.TableWriterNode.WriterTarget;
import io.trino.sql.planner.planprinter.IoPlanPrinter.FormattedMarker.Bound;
import io.trino.sql.planner.planprinter.IoPlanPrinter.IoPlan.IoPlanBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IoPlanPrinter
{
    private final Plan plan;
    private final PlannerContext plannerContext;
    private final Session session;
    private final ValuePrinter valuePrinter;

    private IoPlanPrinter(Plan plan, PlannerContext plannerContext, Session session)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.valuePrinter = new ValuePrinter(plannerContext.getMetadata(), plannerContext.getFunctionManager(), session);
    }

    /**
     * @throws io.trino.NotInTransactionException if called without an active transaction
     */
    public static String textIoPlan(Plan plan, PlannerContext plannerContext, Session session)
    {
        return new IoPlanPrinter(plan, plannerContext, session).print();
    }

    private String print()
    {
        IoPlanBuilder ioPlanBuilder = new IoPlanBuilder(plan);
        plan.getRoot().accept(new IoPlanVisitor(), ioPlanBuilder);
        return jsonCodec(IoPlan.class).toJson(ioPlanBuilder.build());
    }

    public static class IoPlan
    {
        private final Set<TableColumnInfo> inputTableColumnInfos;
        private final Optional<CatalogSchemaTableName> outputTable;
        private final EstimatedStatsAndCost estimate;

        @JsonCreator
        public IoPlan(
                @JsonProperty("inputTableColumnInfos") Set<TableColumnInfo> inputTableColumnInfos,
                @JsonProperty("outputTable") Optional<CatalogSchemaTableName> outputTable,
                @JsonProperty("estimate") EstimatedStatsAndCost estimate)
        {
            this.inputTableColumnInfos = ImmutableSet.copyOf(requireNonNull(inputTableColumnInfos, "inputTableColumnInfos is null"));
            this.outputTable = requireNonNull(outputTable, "outputTable is null");
            this.estimate = requireNonNull(estimate, "estimate is null");
        }

        @JsonProperty
        public Set<TableColumnInfo> getInputTableColumnInfos()
        {
            return inputTableColumnInfos;
        }

        @JsonProperty
        public Optional<CatalogSchemaTableName> getOutputTable()
        {
            return outputTable;
        }

        @JsonProperty
        public EstimatedStatsAndCost getEstimate()
        {
            return estimate;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            IoPlan o = (IoPlan) obj;
            return Objects.equals(inputTableColumnInfos, o.inputTableColumnInfos) &&
                    Objects.equals(outputTable, o.outputTable);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(inputTableColumnInfos, outputTable);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("inputTableColumnInfos", inputTableColumnInfos)
                    .add("outputTable", outputTable)
                    .add("estimate", estimate)
                    .toString();
        }

        protected static class IoPlanBuilder
        {
            private final Plan plan;
            private final Set<TableColumnInfo> inputTableColumnInfos;
            private Optional<CatalogSchemaTableName> outputTable;

            private IoPlanBuilder(Plan plan)
            {
                this.plan = plan;
                this.inputTableColumnInfos = new HashSet<>();
                this.outputTable = Optional.empty();
            }

            private IoPlanBuilder addInputTableColumnInfo(TableColumnInfo tableColumnInfo)
            {
                inputTableColumnInfos.add(tableColumnInfo);
                return this;
            }

            private IoPlanBuilder setOutputTable(CatalogSchemaTableName outputTable)
            {
                this.outputTable = Optional.of(outputTable);
                return this;
            }

            private IoPlan build()
            {
                return new IoPlan(inputTableColumnInfos, outputTable, getEstimatedStatsAndCost());
            }

            private EstimatedStatsAndCost getEstimatedStatsAndCost()
            {
                PlanNode root = plan.getRoot();
                StatsAndCosts statsAndCosts = plan.getStatsAndCosts();
                PlanNodeStatsEstimate statsEstimate = statsAndCosts.getStats().get(root.getId());
                PlanCostEstimate costEstimate = statsAndCosts.getCosts().get(root.getId());
                return new EstimatedStatsAndCost(
                        statsEstimate.getOutputRowCount(),
                        statsEstimate.getOutputSizeInBytes(root.getOutputSymbols(), plan.getTypes()),
                        costEstimate.getCpuCost(),
                        costEstimate.getMaxMemory(),
                        costEstimate.getNetworkCost());
            }
        }

        public static class TableColumnInfo
        {
            private final CatalogSchemaTableName table;
            private final Constraint constraint;
            private final EstimatedStatsAndCost estimate;

            @JsonCreator
            public TableColumnInfo(
                    @JsonProperty("table") CatalogSchemaTableName table,
                    @JsonProperty("constraint") Constraint constraint,
                    @JsonProperty("estimate") EstimatedStatsAndCost estimate)
            {
                this.table = requireNonNull(table, "table is null");
                this.constraint = requireNonNull(constraint, "constraint is null");
                this.estimate = requireNonNull(estimate, "estimate is null");
            }

            @JsonProperty
            public CatalogSchemaTableName getTable()
            {
                return table;
            }

            @JsonProperty
            public Constraint getConstraint()
            {
                return constraint;
            }

            @JsonProperty
            public EstimatedStatsAndCost getEstimate()
            {
                return estimate;
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj) {
                    return true;
                }
                if (obj == null || getClass() != obj.getClass()) {
                    return false;
                }
                TableColumnInfo o = (TableColumnInfo) obj;
                return Objects.equals(table, o.table) &&
                        Objects.equals(constraint, o.constraint) &&
                        Objects.equals(estimate, o.estimate);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(table, constraint, estimate);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("table", table)
                        .add("constraint", constraint)
                        .add("estimate", estimate)
                        .toString();
            }
        }
    }

    public static class Constraint
    {
        private final boolean isNone;
        private final Set<ColumnConstraint> columnConstraints;

        @JsonCreator
        public Constraint(
                @JsonProperty("none") boolean isNone,
                @JsonProperty("columnConstraints") Set<ColumnConstraint> columnConstraints)
        {
            this.isNone = isNone;
            this.columnConstraints = ImmutableSet.copyOf(requireNonNull(columnConstraints, "columnConstraints is null"));
        }

        @JsonProperty
        public boolean isNone()
        {
            return isNone;
        }

        @JsonProperty
        public Set<ColumnConstraint> getColumnConstraints()
        {
            return columnConstraints;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Constraint o = (Constraint) obj;
            return Objects.equals(isNone, o.isNone) &&
                    Objects.equals(columnConstraints, o.columnConstraints);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(isNone, columnConstraints);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("none", isNone)
                    .add("columnConstraints", columnConstraints)
                    .toString();
        }
    }

    public static class ColumnConstraint
    {
        private final String columnName;
        private final Type type;
        private final FormattedDomain domain;

        @JsonCreator
        public ColumnConstraint(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("type") Type type,
                @JsonProperty("domain") FormattedDomain domain)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.type = requireNonNull(type, "type is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public FormattedDomain getDomain()
        {
            return domain;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ColumnConstraint o = (ColumnConstraint) obj;
            return Objects.equals(columnName, o.columnName) &&
                    Objects.equals(type, o.type) &&
                    Objects.equals(domain, o.domain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName, type, domain);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("columnName", columnName)
                    .add("typeSignature", type)
                    .add("domain", domain)
                    .toString();
        }
    }

    public static class EstimatedStatsAndCost
    {
        private final double outputRowCount;
        private final double outputSizeInBytes;
        private final double cpuCost;
        private final double maxMemory;
        private final double networkCost;

        @JsonCreator
        public EstimatedStatsAndCost(
                @JsonProperty("outputRowCount") double outputRowCount,
                @JsonProperty("outputSizeInBytes") double outputSizeInBytes,
                @JsonProperty("cpuCost") double cpuCost,
                @JsonProperty("maxMemory") double maxMemory,
                @JsonProperty("networkCost") double networkCost)
        {
            this.outputRowCount = outputRowCount;
            this.outputSizeInBytes = outputSizeInBytes;
            this.cpuCost = cpuCost;
            this.maxMemory = maxMemory;
            this.networkCost = networkCost;
        }

        @JsonProperty
        public double getOutputRowCount()
        {
            return outputRowCount;
        }

        @JsonProperty
        public double getOutputSizeInBytes()
        {
            return outputSizeInBytes;
        }

        @JsonProperty
        public double getCpuCost()
        {
            return cpuCost;
        }

        @JsonProperty
        public double getMaxMemory()
        {
            return maxMemory;
        }

        @JsonProperty
        public double getNetworkCost()
        {
            return networkCost;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EstimatedStatsAndCost o = (EstimatedStatsAndCost) obj;
            return Objects.equals(outputRowCount, o.outputRowCount) &&
                    Objects.equals(outputSizeInBytes, o.outputSizeInBytes) &&
                    Objects.equals(cpuCost, o.cpuCost) &&
                    Objects.equals(maxMemory, o.maxMemory) &&
                    Objects.equals(networkCost, o.networkCost);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(outputRowCount, outputSizeInBytes, cpuCost, maxMemory, networkCost);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("outputRowCount", outputRowCount)
                    .add("outputSizeInBytes", outputSizeInBytes)
                    .add("cpuCost", cpuCost)
                    .add("maxMemory", maxMemory)
                    .add("networkCost", networkCost)
                    .toString();
        }
    }

    public static class FormattedDomain
    {
        private final boolean nullsAllowed;
        private final Set<FormattedRange> ranges;

        @JsonCreator
        public FormattedDomain(
                @JsonProperty("nullsAllowed") boolean nullsAllowed,
                @JsonProperty("ranges") Set<FormattedRange> ranges)
        {
            this.nullsAllowed = nullsAllowed;
            this.ranges = ImmutableSet.copyOf(requireNonNull(ranges, "ranges is null"));
        }

        @JsonProperty
        public boolean isNullsAllowed()
        {
            return nullsAllowed;
        }

        @JsonProperty
        public Set<FormattedRange> getRanges()
        {
            return ranges;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedDomain o = (FormattedDomain) obj;
            return Objects.equals(nullsAllowed, o.nullsAllowed) &&
                    Objects.equals(ranges, o.ranges);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nullsAllowed, ranges);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("nullsAllowed", nullsAllowed)
                    .add("ranges", ranges)
                    .toString();
        }
    }

    public static class FormattedRange
    {
        private final FormattedMarker low;
        private final FormattedMarker high;

        @JsonCreator
        public FormattedRange(
                @JsonProperty("low") FormattedMarker low,
                @JsonProperty("high") FormattedMarker high)
        {
            this.low = requireNonNull(low, "low is null");
            this.high = requireNonNull(high, "high is null");
        }

        @JsonProperty
        public FormattedMarker getLow()
        {
            return low;
        }

        @JsonProperty
        public FormattedMarker getHigh()
        {
            return high;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedRange o = (FormattedRange) obj;
            return Objects.equals(low, o.low) &&
                    Objects.equals(high, o.high);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(low, high);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("low", low)
                    .add("high", high)
                    .toString();
        }
    }

    public static class FormattedMarker
    {
        public enum Bound
        {
            BELOW,   // lower than the value, but infinitesimally close to the value
            EXACTLY, // exactly the value
            ABOVE    // higher than the value, but infinitesimally close to the value
        }

        private final Optional<String> value;
        private final Bound bound;

        @JsonCreator
        public FormattedMarker(
                @JsonProperty("value") Optional<String> value,
                @JsonProperty("bound") Bound bound)
        {
            this.value = requireNonNull(value, "value is null");
            this.bound = requireNonNull(bound, "bound is null");
        }

        @JsonProperty
        public Optional<String> getValue()
        {
            return value;
        }

        @JsonProperty
        public Bound getBound()
        {
            return bound;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FormattedMarker o = (FormattedMarker) obj;
            return Objects.equals(value, o.value) &&
                    bound == o.bound;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, bound);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("value", value)
                    .add("bound", bound)
                    .toString();
        }
    }

    private class IoPlanVisitor
            extends PlanVisitor<Void, IoPlanBuilder>
    {
        @Override
        protected Void visitPlan(PlanNode node, IoPlanBuilder context)
        {
            return processChildren(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, IoPlanBuilder context)
        {
            PlanNode source = node.getSource();
            if (source instanceof TableScanNode tableScanNode) {
                DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.getExtractionResult(
                        plannerContext,
                        session,
                        node.getPredicate(),
                        plan.getTypes());
                TupleDomain<ColumnHandle> filterDomain = decomposedPredicate.getTupleDomain()
                        .transformKeys(tableScanNode.getAssignments()::get);
                addInputTableConstraints(filterDomain, tableScanNode, context);
                return null;
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, IoPlanBuilder context)
        {
            addInputTableConstraints(TupleDomain.all(), node, context);
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, IoPlanBuilder context)
        {
            WriterTarget writerTarget = node.getTarget();
            if (writerTarget instanceof CreateTarget target) {
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogHandle().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof InsertTarget target) {
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogHandle().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof MergeTarget target) {
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogHandle().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof TableWriterNode.RefreshMaterializedViewTarget target) {
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getInsertHandle().getCatalogHandle().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof CreateReference || writerTarget instanceof InsertReference) {
                throw new IllegalStateException(format("%s should not appear in final plan", writerTarget.getClass().getSimpleName()));
            }
            else {
                throw new IllegalStateException(format("Unknown WriterTarget subclass %s", writerTarget.getClass().getSimpleName()));
            }
            return processChildren(node, context);
        }

        private void addInputTableConstraints(TupleDomain<ColumnHandle> filterDomain, TableScanNode tableScan, IoPlanBuilder context)
        {
            TableHandle table = tableScan.getTable();
            TableMetadata tableMetadata = plannerContext.getMetadata().getTableMetadata(session, table);
            TupleDomain<ColumnHandle> predicateDomain = plannerContext.getMetadata().getTableProperties(session, table).getPredicate();
            EstimatedStatsAndCost estimatedStatsAndCost = getEstimatedStatsAndCost(tableScan);
            context.addInputTableColumnInfo(
                    new IoPlan.TableColumnInfo(
                            new CatalogSchemaTableName(
                                    tableMetadata.getCatalogName(),
                                    tableMetadata.getTable().getSchemaName(),
                                    tableMetadata.getTable().getTableName()),
                            parseConstraint(table, predicateDomain.intersect(filterDomain)),
                            estimatedStatsAndCost));
        }

        private EstimatedStatsAndCost getEstimatedStatsAndCost(TableScanNode node)
        {
            StatsAndCosts statsAndCosts = plan.getStatsAndCosts();
            PlanNodeStatsEstimate stats = statsAndCosts.getStats().get(node.getId());
            PlanCostEstimate cost = statsAndCosts.getCosts().get(node.getId());

            EstimatedStatsAndCost estimatedStatsAndCost = new EstimatedStatsAndCost(
                    stats.getOutputRowCount(),
                    stats.getOutputSizeInBytes(node.getOutputSymbols(), plan.getTypes()),
                    cost.getCpuCost(),
                    cost.getMaxMemory(),
                    cost.getNetworkCost());
            return estimatedStatsAndCost;
        }

        private Constraint parseConstraint(TableHandle tableHandle, TupleDomain<ColumnHandle> constraint)
        {
            if (constraint.isNone()) {
                return new Constraint(true, ImmutableSet.of());
            }
            ImmutableSet.Builder<ColumnConstraint> columnConstraints = ImmutableSet.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().orElseThrow().entrySet()) {
                ColumnMetadata columnMetadata = plannerContext.getMetadata().getColumnMetadata(session, tableHandle, entry.getKey());
                columnConstraints.add(new ColumnConstraint(
                        columnMetadata.getName(),
                        columnMetadata.getType(),
                        parseDomain(entry.getValue().simplify())));
            }
            return new Constraint(false, columnConstraints.build());
        }

        private FormattedDomain parseDomain(Domain domain)
        {
            ImmutableSet.Builder<FormattedRange> formattedRanges = ImmutableSet.builder();
            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> formattedRanges.addAll(
                            ranges.getOrderedRanges().stream()
                                    .map(this::formatRange)
                                    .collect(toImmutableSet())),
                    discreteValues -> formattedRanges.addAll(
                            discreteValues.getValues().stream()
                                    .map(value -> valuePrinter.castToVarcharOrFail(type, value))
                                    .map(value -> new FormattedMarker(Optional.of(value), Bound.EXACTLY))
                                    .map(marker -> new FormattedRange(marker, marker))
                                    .collect(toImmutableSet())),
                    allOrNone -> {
                        throw new IllegalStateException("Unreachable AllOrNone consumer");
                    });

            return new FormattedDomain(domain.isNullAllowed(), formattedRanges.build());
        }

        private FormattedRange formatRange(Range range)
        {
            FormattedMarker low = range.isLowUnbounded()
                    ? new FormattedMarker(Optional.empty(), Bound.ABOVE)
                    : new FormattedMarker(
                    Optional.of(valuePrinter.castToVarcharOrFail(range.getType(), range.getLowBoundedValue())),
                    range.isLowInclusive() ? Bound.EXACTLY : Bound.ABOVE);

            FormattedMarker high = range.isHighUnbounded()
                    ? new FormattedMarker(Optional.empty(), Bound.BELOW)
                    : new FormattedMarker(
                    Optional.of(valuePrinter.castToVarcharOrFail(range.getType(), range.getHighBoundedValue())),
                    range.isHighInclusive() ? Bound.EXACTLY : Bound.BELOW);

            return new FormattedRange(low, high);
        }

        private Void processChildren(PlanNode node, IoPlanBuilder context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }
    }
}

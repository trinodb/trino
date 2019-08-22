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
package io.prestosql.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Marker.Bound;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TableWriterNode.CreateReference;
import io.prestosql.sql.planner.plan.TableWriterNode.CreateTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.DeleteTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.InsertReference;
import io.prestosql.sql.planner.plan.TableWriterNode.InsertTarget;
import io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import io.prestosql.sql.planner.planprinter.IoPlanPrinter.IoPlan.IoPlanBuilder;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.spi.predicate.Marker.Bound.EXACTLY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IoPlanPrinter
{
    private final Plan plan;
    private final Metadata metadata;
    private final Session session;
    private final ValuePrinter valuePrinter;

    private IoPlanPrinter(Plan plan, Metadata metadata, Session session)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.valuePrinter = new ValuePrinter(metadata, session);
    }

    /**
     * @throws io.prestosql.NotInTransactionException if called without an active transaction
     */
    public static String textIoPlan(Plan plan, Metadata metadata, Session session)
    {
        return new IoPlanPrinter(plan, metadata, session).print();
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
            private Set<TableColumnInfo> inputTableColumnInfos;
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
            private final Set<ColumnConstraint> columnConstraints;
            private final EstimatedStatsAndCost estimate;

            @JsonCreator
            public TableColumnInfo(
                    @JsonProperty("table") CatalogSchemaTableName table,
                    @JsonProperty("columnConstraints") Set<ColumnConstraint> columnConstraints,
                    @JsonProperty("estimate") EstimatedStatsAndCost estimate)
            {
                this.table = requireNonNull(table, "table is null");
                this.columnConstraints = requireNonNull(columnConstraints, "columnConstraints is null");
                this.estimate = requireNonNull(estimate, "estimate is null");
            }

            @JsonProperty
            public CatalogSchemaTableName getTable()
            {
                return table;
            }

            @JsonProperty
            public Set<ColumnConstraint> getColumnConstraints()
            {
                return columnConstraints;
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
                        Objects.equals(columnConstraints, o.columnConstraints) &&
                        Objects.equals(estimate, o.estimate);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(table, columnConstraints, estimate);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("table", table)
                        .add("columnConstraints", columnConstraints)
                        .add("estimate", estimate)
                        .toString();
            }
        }
    }

    public static class ColumnConstraint
    {
        private final String columnName;
        private final TypeSignature typeSignature;
        private final FormattedDomain domain;

        @JsonCreator
        public ColumnConstraint(
                @JsonProperty("columnName") String columnName,
                @JsonProperty("typeSignature") TypeSignature typeSignature,
                @JsonProperty("domain") FormattedDomain domain)
        {
            this.columnName = requireNonNull(columnName, "columnName is null");
            this.typeSignature = requireNonNull(typeSignature, "type is null");
            this.domain = requireNonNull(domain, "domain is null");
        }

        @JsonProperty
        public String getColumnName()
        {
            return columnName;
        }

        @JsonProperty
        public TypeSignature getTypeSignature()
        {
            return typeSignature;
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
                    Objects.equals(typeSignature, o.typeSignature) &&
                    Objects.equals(domain, o.domain);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(columnName, typeSignature, domain);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("columnName", columnName)
                    .add("typeSignature", typeSignature)
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
            this.outputRowCount = requireNonNull(outputRowCount, "outputRowCount is null");
            this.outputSizeInBytes = requireNonNull(outputSizeInBytes, "outputSizeInBytes is null");
            this.cpuCost = requireNonNull(cpuCost, "cpuCost is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.networkCost = requireNonNull(networkCost, "networkCost is null");
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
                    Objects.equals(bound, o.bound);
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
        public Void visitTableScan(TableScanNode node, IoPlanBuilder context)
        {
            TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
            TupleDomain<ColumnHandle> predicate = metadata.getTableProperties(session, node.getTable()).getPredicate();
            EstimatedStatsAndCost estimatedStatsAndCost = getEstimatedStatsAndCost(node);
            context.addInputTableColumnInfo(
                    new IoPlan.TableColumnInfo(
                        new CatalogSchemaTableName(
                                tableMetadata.getCatalogName().getCatalogName(),
                                tableMetadata.getTable().getSchemaName(),
                                tableMetadata.getTable().getTableName()),
                        parseConstraints(node.getTable(), predicate),
                        estimatedStatsAndCost));
            return null;
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, IoPlanBuilder context)
        {
            WriterTarget writerTarget = node.getTarget();
            if (writerTarget instanceof CreateTarget) {
                CreateTarget target = (CreateTarget) writerTarget;
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogName().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof InsertTarget) {
                InsertTarget target = (InsertTarget) writerTarget;
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogName().getCatalogName(),
                        target.getSchemaTableName().getSchemaName(),
                        target.getSchemaTableName().getTableName()));
            }
            else if (writerTarget instanceof DeleteTarget) {
                DeleteTarget target = (DeleteTarget) writerTarget;
                context.setOutputTable(new CatalogSchemaTableName(
                        target.getHandle().getCatalogName().getCatalogName(),
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

        private Set<ColumnConstraint> parseConstraints(TableHandle tableHandle, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            ImmutableSet.Builder<ColumnConstraint> columnConstraints = ImmutableSet.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, entry.getKey());
                columnConstraints.add(new ColumnConstraint(
                        columnMetadata.getName(),
                        columnMetadata.getType().getTypeSignature(),
                        parseDomain(entry.getValue().simplify())));
            }
            return columnConstraints.build();
        }

        private FormattedDomain parseDomain(Domain domain)
        {
            ImmutableSet.Builder<FormattedRange> formattedRanges = ImmutableSet.builder();
            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> formattedRanges.addAll(
                            ranges.getOrderedRanges().stream()
                                    .map(range -> new FormattedRange(formatMarker(range.getLow()), formatMarker(range.getHigh())))
                                    .collect(toImmutableSet())),
                    discreteValues -> formattedRanges.addAll(
                            discreteValues.getValues().stream()
                                    .map(value -> valuePrinter.castToVarcharOrFail(type, value))
                                    .map(value -> new FormattedMarker(Optional.of(value), EXACTLY))
                                    .map(marker -> new FormattedRange(marker, marker))
                                    .collect(toImmutableSet())),
                    allOrNone -> {
                        throw new IllegalStateException("Unreachable AllOrNone consumer");
                    });

            return new FormattedDomain(domain.isNullAllowed(), formattedRanges.build());
        }

        private FormattedMarker formatMarker(Marker marker)
        {
            if (!marker.getValueBlock().isPresent()) {
                return new FormattedMarker(Optional.empty(), marker.getBound());
            }
            return new FormattedMarker(Optional.of(valuePrinter.castToVarcharOrFail(marker.getType(), marker.getValue())), marker.getBound());
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

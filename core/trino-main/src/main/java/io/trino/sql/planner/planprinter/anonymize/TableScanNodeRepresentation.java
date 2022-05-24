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

package io.trino.sql.planner.planprinter.anonymize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.execution.TableInfo;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.ObjectType.CATALOG;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.ObjectType.COLUMN;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.ObjectType.SCHEMA;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.ObjectType.TABLE;
import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class TableScanNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final Optional<String> connector;
    private final String catalog;
    private final String schema;
    private final String table;
    private final Map<Symbol, String> assignments;
    private final Optional<Map<String, DomainRepresentation>> enforcedConstraint;
    private final Optional<Map<String, DomainRepresentation>> predicate;

    @JsonCreator
    public TableScanNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("connector") Optional<String> connector,
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("assignments") Map<Symbol, String> assignments,
            @JsonProperty("enforcedConstraint") Optional<Map<String, DomainRepresentation>> enforcedConstraint,
            @JsonProperty("predicate") Optional<Map<String, DomainRepresentation>> predicate)
    {
        super(id, outputLayout, ImmutableList.of());
        this.connector = requireNonNull(connector, "connector is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public Optional<String> getConnector()
    {
        return connector;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public Map<Symbol, String> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public Optional<Map<String, DomainRepresentation>> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    @JsonProperty
    public Optional<Map<String, DomainRepresentation>> getPredicate()
    {
        return predicate;
    }

    public static TableScanNodeRepresentation fromPlanNode(TableScanNode node, TypeProvider typeProvider, TableInfo tableInfo)
    {
        return new TableScanNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                tableInfo.getConnectorName(),
                anonymize(tableInfo.getTableName().getCatalogName(), CATALOG),
                anonymize(tableInfo.getTableName().getSchemaName(), SCHEMA),
                anonymize(tableInfo.getTableName().getObjectName(), TABLE),
                node.getAssignments().entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> anonymize(entry.getKey()),
                                entry -> anonymize(entry.getValue(), COLUMN))),
                anonymize(node.getEnforcedConstraint()),
                anonymize(tableInfo.getPredicate()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableScanNodeRepresentation that = (TableScanNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && connector.equals(that.connector)
                && catalog.equals(that.catalog)
                && schema.equals(that.schema)
                && table.equals(that.table)
                && assignments.equals(that.assignments)
                && enforcedConstraint.equals(that.enforcedConstraint)
                && predicate.equals(that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                getId(),
                getOutputLayout(),
                getSources(),
                connector,
                catalog,
                schema,
                table,
                assignments,
                enforcedConstraint,
                predicate);
    }

    public static class DomainRepresentation
    {
        private final String valueSetClass;
        private final String valueSetType;
        private final Optional<Integer> valueSetCount;
        private final boolean nullAllowed;
        private final boolean none;
        private final boolean all;
        private final boolean singleValue;

        @JsonCreator
        public DomainRepresentation(
                @JsonProperty("valueSetClass") String valueSetClass,
                @JsonProperty("valueSetType") String valueSetType,
                @JsonProperty("valueSetCount") Optional<Integer> valueSetCount,
                @JsonProperty("nullAllowed") boolean nullAllowed,
                @JsonProperty("none") boolean none,
                @JsonProperty("all") boolean all,
                @JsonProperty("singleValue") boolean singleValue)
        {
            this.valueSetClass = requireNonNull(valueSetClass, "valueSetClass is null");
            this.valueSetType = requireNonNull(valueSetType, "valueSetType is null");
            this.valueSetCount = requireNonNull(valueSetCount, "valueSetCount is null");
            this.nullAllowed = nullAllowed;
            this.none = none;
            this.all = all;
            this.singleValue = singleValue;
        }

        @JsonProperty
        public String getValueSetClass()
        {
            return valueSetClass;
        }

        @JsonProperty
        public String getValueSetType()
        {
            return valueSetType;
        }

        @JsonProperty
        public Optional<Integer> getValueSetCount()
        {
            return valueSetCount;
        }

        @JsonProperty
        public boolean isNullAllowed()
        {
            return nullAllowed;
        }

        @JsonProperty
        public boolean isNone()
        {
            return none;
        }

        @JsonProperty
        public boolean isAll()
        {
            return all;
        }

        @JsonProperty
        public boolean isSingleValue()
        {
            return singleValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DomainRepresentation)) {
                return false;
            }
            DomainRepresentation that = (DomainRepresentation) o;
            return nullAllowed == that.nullAllowed
                    && none == that.none
                    && all == that.all
                    && singleValue == that.singleValue
                    && valueSetClass.equals(that.valueSetClass)
                    && valueSetType.equals(that.valueSetType)
                    && valueSetCount.equals(that.valueSetCount);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(valueSetClass, valueSetType, valueSetCount, nullAllowed, none, all, singleValue);
        }
    }
}

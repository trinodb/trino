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
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.planprinter.TypedSymbol;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.sql.planner.planprinter.anonymize.AnonymizationUtils.anonymize;
import static java.util.Objects.requireNonNull;

public class ValuesNodeRepresentation
        extends AnonymizedNodeRepresentation
{
    private final int rowCount;
    private final Optional<List<String>> rows;

    @JsonCreator
    public ValuesNodeRepresentation(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputLayout") List<TypedSymbol> outputLayout,
            @JsonProperty("rowCount") int rowCount,
            @JsonProperty("rows") Optional<List<String>> rows)
    {
        super(id, outputLayout, ImmutableList.of());
        this.rowCount = rowCount;
        this.rows = requireNonNull(rows, "rows is null");
    }

    @JsonProperty
    public int getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Optional<List<String>> getRows()
    {
        return rows;
    }

    public static ValuesNodeRepresentation fromPlanNode(ValuesNode node, TypeProvider typeProvider)
    {
        return new ValuesNodeRepresentation(
                node.getId(),
                anonymize(node.getOutputSymbols(), typeProvider),
                node.getRowCount(),
                node.getRows().map(AnonymizationUtils::anonymizeAndUnResolveExpressions));
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
        ValuesNodeRepresentation that = (ValuesNodeRepresentation) o;
        return getId().equals(that.getId())
                && getOutputLayout().equals(that.getOutputLayout())
                && getSources().equals(that.getSources())
                && rowCount == that.rowCount
                && rows.equals(that.rows);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getOutputLayout(), getSources(), rowCount, rows);
    }
}

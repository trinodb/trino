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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.Statistics;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.Attributes.CONSTRAINT;
import static io.trino.sql.dialect.trino.Attributes.STATISTICS;
import static io.trino.sql.dialect.trino.Attributes.TABLE_HANDLE;
import static io.trino.sql.dialect.trino.Attributes.UPDATE_TARGET;
import static io.trino.sql.dialect.trino.Attributes.USE_CONNECTOR_NODE_PARTITIONING;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.assignRelationRowTypeFieldNames;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static java.util.Objects.requireNonNull;

public class TableScan
        extends Operation
{
    private static final String NAME = "table_scan";

    private final Result result;
    private final Map<AttributeKey, Object> attributes;

    public TableScan(
            String resultName,
            Type rowType, // RowType or EMPTY_ROW
            TableHandle tableHandle,
            List<ColumnHandle> columnHandles, // list of ColumnHandle backing the output fields. It may contain duplicates
            TupleDomain<ColumnHandle> enforcedConstraint, // TODO do not send to workers
            Optional<Statistics> statistics, // TODO do not send to workers
            boolean updateTarget,
            Optional<Boolean> useConnectorNodePartitioning)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(rowType, "rowType is null");
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        requireNonNull(statistics, "statistics is null");
        requireNonNull(useConnectorNodePartitioning, "useConnectorNodePartitioning is null");

        if (rowType.getTypeParameters().size() != columnHandles.size()) {
            throw new TrinoException(IR_ERROR, "column handles do not match the output type of TableScan operation");
        }

        validateEnforcedConstraint(enforcedConstraint, columnHandles);

        List<Type> outputTypes = rowType.getTypeParameters();
        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(assignRelationRowTypeFieldNames(RowType.anonymous(outputTypes)))));
        }

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        TABLE_HANDLE.putAttribute(attributes, tableHandle);
        COLUMN_HANDLES.putAttribute(attributes, columnHandles);
        CONSTRAINT.putAttribute(attributes, enforcedConstraint);
        statistics.ifPresent(estimate -> STATISTICS.putAttribute(attributes, estimate));
        UPDATE_TARGET.putAttribute(attributes, updateTarget);
        useConnectorNodePartitioning.ifPresent(usePartitioning -> USE_CONNECTOR_NODE_PARTITIONING.putAttribute(attributes, usePartitioning));

        this.attributes = attributes.buildOrThrow();
    }

    private static void validateEnforcedConstraint(TupleDomain<ColumnHandle> enforcedConstraint, List<ColumnHandle> columnHandles)
    {
        if (enforcedConstraint.isAll() || enforcedConstraint.isNone()) {
            return;
        }

        Set<ColumnHandle> availableColumns = ImmutableSet.copyOf(columnHandles);
        Set<ColumnHandle> domainColumns = enforcedConstraint.getDomains().orElseThrow().keySet();
        Set<ColumnHandle> unavailableDomainColumns = Sets.difference(domainColumns, availableColumns);

        if (!unavailableDomainColumns.isEmpty()) {
            throw new TrinoException(IR_ERROR, "enforced constraint of TableScan operation references columns that are not available: " + unavailableDomainColumns);
        }
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of();
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty table scan";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (TableScan) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, attributes);
    }
}

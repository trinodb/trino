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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import jakarta.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getCheckConstraints;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnComments;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnInvariants;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnTypes;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsMetadata;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getColumnsNullability;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getExactColumnNames;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.getGeneratedColumnExpressions;
import static java.util.Objects.requireNonNull;

public record DeltaLakeTable(List<DeltaLakeColumn> columns, List<String> constraints)
{
    public DeltaLakeTable
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(constraints, "constraints is null");
        checkArgument(!columns.isEmpty(), "columns must not be empty");

        columns = ImmutableList.copyOf(columns);
        constraints = ImmutableList.copyOf(constraints);
    }

    public DeltaLakeColumn findColumn(String name)
    {
        requireNonNull(name, "name is null");
        return columns.stream().filter(column -> column.name.equals(name)).collect(onlyElement());
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        return new Builder(metadataEntry, protocolEntry);
    }

    public static Builder builder(StructType tableSchema)
    {
        return new Builder(tableSchema);
    }

    public static class Builder
    {
        private final List<DeltaLakeColumn> columns = new ArrayList<>();
        private final List<String> constraints = new ArrayList<>();

        public Builder() {}

        public Builder(MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
        {
            requireNonNull(metadataEntry, "metadataEntry is null");

            Map<String, Object> columnTypes = getColumnTypes(metadataEntry);
            Map<String, Boolean> columnsNullability = getColumnsNullability(metadataEntry);
            Map<String, String> columnComments = getColumnComments(metadataEntry);
            Map<String, Map<String, Object>> columnsMetadata = getColumnsMetadata(metadataEntry);
            Map<String, String> columnGenerations = getGeneratedColumnExpressions(metadataEntry);

            for (String columnName : getExactColumnNames(metadataEntry)) {
                columns.add(new DeltaLakeColumn(columnName,
                        columnTypes.get(columnName),
                        columnsNullability.getOrDefault(columnName, true),
                        columnComments.get(columnName),
                        columnsMetadata.get(columnName),
                        Optional.ofNullable(columnGenerations.get(columnName))));
            }

            constraints.addAll(ImmutableList.<String>builder()
                    .addAll(getCheckConstraints(metadataEntry, protocolEntry).values())
                    .addAll(getColumnInvariants(metadataEntry, protocolEntry).values()) // The internal logic for column invariants in Delta Lake is same as check constraints
                    .build());
        }

        /**
         * Construct builder for Kernel schema object
         *
         * @param tableSchema Table schema in Delta format.
         */
        public Builder(StructType tableSchema)
        {
            requireNonNull(tableSchema, "tableSchema is null");

            for (StructField structField : tableSchema.fields()) {
                columns.add(new DeltaLakeColumn(structField.getName(),
                        structField.getDataType().toJson(),
                        structField.isNullable(),
                        null /* comment */,
                        null /* metadata */,
                        Optional.empty() /* generationExpression */));
            }

            // TODO: Add constraints
        }

        public Builder addColumn(String name, Object type, boolean nullable, @Nullable String comment, @Nullable Map<String, Object> metadata)
        {
            columns.add(new DeltaLakeColumn(name, type, nullable, comment, metadata, Optional.empty()));
            return this;
        }

        public Builder renameColumn(String source, String target)
        {
            checkArgument(columns.stream().noneMatch(column -> column.name.equalsIgnoreCase(target)), "Column already exists: %s", target);

            DeltaLakeColumn column = findColumn(source);
            int index = columns.indexOf(column);
            verify(index >= 0, "Unexpected column index");

            DeltaLakeColumn newColumn = new DeltaLakeColumn(target, column.type, column.nullable, column.comment, column.metadata, column.generationExpression);
            columns.set(index, newColumn);
            return this;
        }

        public Builder removeColumn(String name)
        {
            DeltaLakeColumn column = findColumn(name);
            boolean removed = columns.remove(column);
            checkState(removed, "Failed to remove '%s' from %s", name, columns);
            return this;
        }

        public Builder setColumnComment(String name, @Nullable String comment)
        {
            DeltaLakeColumn oldColumn = findColumn(name);
            DeltaLakeColumn newColumn = new DeltaLakeColumn(oldColumn.name, oldColumn.type, oldColumn.nullable, comment, oldColumn.metadata, oldColumn.generationExpression);
            columns.set(columns.indexOf(oldColumn), newColumn);
            return this;
        }

        public Builder dropNotNullConstraint(String name)
        {
            DeltaLakeColumn oldColumn = findColumn(name);
            verify(!oldColumn.nullable, "Column '%s' is already nullable", name);
            DeltaLakeColumn newColumn = new DeltaLakeColumn(oldColumn.name, oldColumn.type, true, oldColumn.comment, oldColumn.metadata, oldColumn.generationExpression);
            columns.set(columns.indexOf(oldColumn), newColumn);
            return this;
        }

        private DeltaLakeColumn findColumn(String name)
        {
            requireNonNull(name, "name is null");
            return columns.stream().filter(column -> column.name.equals(name)).collect(onlyElement());
        }

        public DeltaLakeTable build()
        {
            return new DeltaLakeTable(columns, constraints);
        }
    }

    public record DeltaLakeColumn(String name, Object type, boolean nullable, @Nullable String comment, @Nullable Map<String, Object> metadata,
                                  Optional<String> generationExpression)
    {
        public DeltaLakeColumn
        {
            checkArgument(!name.isEmpty(), "name is empty");
            requireNonNull(type, "type is null");
            requireNonNull(generationExpression, "generationExpression is null");
        }
    }
}

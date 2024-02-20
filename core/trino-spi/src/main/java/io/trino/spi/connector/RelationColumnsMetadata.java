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
package io.trino.spi.connector;

import io.trino.spi.Experimental;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.connector.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Experimental(eta = "2024-01-01")
public record RelationColumnsMetadata(
        SchemaTableName name,
        Optional<List<ConnectorMaterializedViewDefinition.Column>> materializedViewColumns,
        Optional<List<ConnectorViewDefinition.ViewColumn>> viewColumns,
        Optional<List<ColumnMetadata>> tableColumns,
        boolean redirected)
{
    public RelationColumnsMetadata
    {
        requireNonNull(name, "name is null");
        materializedViewColumns = materializedViewColumns.map(List::copyOf);
        viewColumns = viewColumns.map(List::copyOf);
        tableColumns = tableColumns.map(List::copyOf);

        checkArgument(
                Stream.of(materializedViewColumns.isPresent(), viewColumns.isPresent(), tableColumns.isPresent(), redirected)
                        .filter(value -> value)
                        .count() == 1,
                "Expected exactly one to be true. Use factory methods to ensure correct instantiation");
    }

    public static RelationColumnsMetadata forMaterializedView(SchemaTableName name, List<ConnectorMaterializedViewDefinition.Column> columns)
    {
        return new RelationColumnsMetadata(
                name,
                Optional.of(columns),
                Optional.empty(),
                Optional.empty(),
                false);
    }

    public static RelationColumnsMetadata forView(SchemaTableName name, List<ConnectorViewDefinition.ViewColumn> columns)
    {
        return new RelationColumnsMetadata(
                name,
                Optional.empty(),
                Optional.of(columns),
                Optional.empty(),
                false);
    }

    public static RelationColumnsMetadata forTable(SchemaTableName name, List<ColumnMetadata> columns)
    {
        return new RelationColumnsMetadata(
                name,
                Optional.empty(),
                Optional.empty(),
                Optional.of(columns),
                false);
    }

    public static RelationColumnsMetadata forRedirectedTable(SchemaTableName name)
    {
        return new RelationColumnsMetadata(
                name,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true);
    }
}

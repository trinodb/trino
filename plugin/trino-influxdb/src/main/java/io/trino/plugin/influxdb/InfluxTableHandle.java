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

package io.trino.plugin.influxdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public record InfluxTableHandle(
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("tableName") String tableName,
        @JsonProperty("columns") List<InfluxColumnHandle> columns,
        @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
        @JsonProperty("projections") List<ColumnHandle> projections,
        @JsonProperty("limit") OptionalInt limit)
        implements ConnectorTableHandle
{
    @JsonCreator
    public InfluxTableHandle(
            String schemaName,
            String tableName,
            List<InfluxColumnHandle> columns,
            TupleDomain<ColumnHandle> constraint,
            List<ColumnHandle> projections,
            OptionalInt limit)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.limit = requireNonNull(limit, "limit is null");
    }

    public static InfluxTableHandle of(String schemaName, String tableName)
    {
        return new InfluxTableHandle(schemaName, tableName, ImmutableList.of(), TupleDomain.all(), ImmutableList.of(), OptionalInt.empty());
    }

    public static InfluxTableHandle of(String schemaName, String tableName, List<InfluxColumnHandle> columns)
    {
        return new InfluxTableHandle(schemaName, tableName, columns, TupleDomain.all(), ImmutableList.of(), OptionalInt.empty());
    }

    public InfluxTableHandle withProjections(List<ColumnHandle> projections)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    public InfluxTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    public InfluxTableHandle withLimit(OptionalInt limit)
    {
        return new InfluxTableHandle(
                schemaName,
                tableName,
                columns,
                constraint,
                projections,
                limit);
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}

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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record MongoTableHandle(
        SchemaTableName schemaTableName,
        RemoteTableName remoteTableName,
        Optional<String> filter,
        TupleDomain<ColumnHandle> constraint,
        Set<MongoColumnHandle> projectedColumns,
        OptionalInt limit)
        implements ConnectorTableHandle
{
    public MongoTableHandle(SchemaTableName schemaTableName, RemoteTableName remoteTableName, Optional<String> filter)
    {
        this(schemaTableName, remoteTableName, filter, TupleDomain.all(), ImmutableSet.of(), OptionalInt.empty());
    }

    public MongoTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(constraint, "constraint is null");
        projectedColumns = ImmutableSet.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        requireNonNull(limit, "limit is null");
    }

    public MongoTableHandle withProjectedColumns(Set<MongoColumnHandle> projectedColumns)
    {
        return new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                filter,
                constraint,
                projectedColumns,
                limit);
    }

    public MongoTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new MongoTableHandle(
                schemaTableName,
                remoteTableName,
                filter,
                constraint,
                projectedColumns,
                limit);
    }
}

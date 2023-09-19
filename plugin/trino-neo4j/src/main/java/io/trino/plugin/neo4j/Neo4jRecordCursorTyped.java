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
package io.trino.plugin.neo4j;

import io.airlift.slice.Slice;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class Neo4jRecordCursorTyped
        extends Neo4jRecordCursorBase
{
    private final List<Type> columnTypes;

    public Neo4jRecordCursorTyped(
            Neo4jClient client,
            Neo4jTypeManager typeManager,
            Optional<String> databaseName,
            String cypher,
            List<Neo4jColumnHandle> columnHandles)
    {
        super(client, typeManager, databaseName, cypher);
        this.columnTypes = columnHandles.stream().map(Neo4jColumnHandle::getColumnType).collect(toImmutableList());
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < this.columnTypes.size(), "Invalid field index");

        return this.columnTypes.get(field);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.typeManager.toBoolean(record.get(field), this.columnTypes.get(field));
    }

    @Override
    public long getLong(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.typeManager.toLong(record.get(field), this.columnTypes.get(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.typeManager.toDouble(record.get(field), this.columnTypes.get(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        if (this.columnTypes.size() == 1 && this.getType(field).equals(this.typeManager.getDynamicResultColumn())) {
            return typeManager.toJson(record);
        }

        return this.typeManager.toSlice(record.get(field), this.columnTypes.get(field));
    }

    @Override
    public Object getObject(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.typeManager.toObject(record.get(field), getType(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return record.get(field).isNull();
    }
}

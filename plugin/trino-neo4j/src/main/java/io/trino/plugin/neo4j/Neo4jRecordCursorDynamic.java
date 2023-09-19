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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class Neo4jRecordCursorDynamic
        extends Neo4jRecordCursorBase
{
    public Neo4jRecordCursorDynamic(
            Neo4jClient client,
            Neo4jTypeManager typeManager,
            Optional<String> databaseName,
            String cypher)
    {
        super(client, typeManager, databaseName, cypher);
    }

    @Override
    public Type getType(int field)
    {
        verify(field == 0, "should only be called for a single field, was: %s", field);

        return this.typeManager.getJsonType();
    }

    @Override
    public boolean isNull(int field)
    {
        verify(field == 0, "should only be called for a single field, was: %s", field);
        checkState(this.session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.record.get(field).isNull();
    }

    @Override
    public Slice getSlice(int field)
    {
        verify(field == 0, "should only be called for a single field, was: %s", field);
        checkState(this.session.isOpen(), "cursor is closed");
        requireNonNull(result, "result is null");
        requireNonNull(record, "record is null");

        return this.typeManager.toJson(this.record.asMap());
    }
}

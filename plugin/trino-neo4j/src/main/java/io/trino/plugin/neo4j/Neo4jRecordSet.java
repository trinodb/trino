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

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class Neo4jRecordSet
        implements RecordSet
{
    private final Neo4jClient client;
    private final Neo4jTypeManager typeManager;
    private final Optional<String> databaseName;
    private final String cypher;
    private final List<Neo4jColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public Neo4jRecordSet(
            Neo4jClient client,
            Neo4jTypeManager typeManager,
            Optional<String> databaseName,
            String cypher,
            List<Neo4jColumnHandle> columnHandles)
    {
        this.client = client;
        this.typeManager = typeManager;
        this.databaseName = databaseName;
        this.cypher = cypher;
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(Neo4jColumnHandle::getColumnType).collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return this.columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        if (this.columnHandles.size() == 1 && this.columnHandles.get(0).equals(this.typeManager.getDynamicResultColumn())) {
            return new Neo4jRecordCursorDynamic(this.client, this.typeManager, this.databaseName, this.cypher);
        }

        return new Neo4jRecordCursorTyped(this.client, this.typeManager, this.databaseName, this.cypher, this.columnHandles);
    }
}

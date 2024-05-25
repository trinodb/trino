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
package io.trino.plugin.accumulo.metadata;

import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an Accumulo view in Trino.
 */
public record AccumuloView(String schema, String table, String data)
{
    public AccumuloView
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(data, "data is null");
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schema, table);
    }
}

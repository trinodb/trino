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
package io.trino.plugin.deltalake.metastore;

import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record DeltaMetastoreTable(
        SchemaTableName schemaTableName,
        boolean managed,
        String location)
{
    public DeltaMetastoreTable
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(location, "location is null");
    }
}

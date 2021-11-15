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
package io.trino.plugin.jdbc.mapping;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ToRemoteColumnMappingKey
{
    private final String remoteSchema;
    private final String remoteTable;
    private final String mapping;

    public ToRemoteColumnMappingKey(
            String remoteSchema,
            String remoteTable,
            String mapping)
    {
        this.remoteSchema = requireNonNull(remoteSchema, "remoteSchema is null");
        this.remoteTable = requireNonNull(remoteTable, "remoteTable is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ToRemoteColumnMappingKey that = (ToRemoteColumnMappingKey) o;
        return remoteSchema.equals(that.remoteSchema) && remoteTable.equals(that.remoteTable) && mapping.equals(that.mapping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteSchema, remoteTable, mapping);
    }
}

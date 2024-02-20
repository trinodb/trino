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
package io.trino.plugin.base.mapping;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TableMappingRule
{
    private final String remoteSchema;
    private final String remoteTable;
    private final String mapping;

    @JsonCreator
    public TableMappingRule(
            @JsonProperty String remoteSchema,
            @JsonProperty String remoteTable,
            @JsonProperty String mapping)
    {
        this.remoteSchema = requireNonNull(remoteSchema, "remoteSchema is null");
        this.remoteTable = requireNonNull(remoteTable, "remoteTable is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
        checkArgument(mapping.toLowerCase(ENGLISH).equals(mapping), "Mapping is not lower cased: %s", mapping);
    }

    @JsonProperty
    public String getRemoteSchema()
    {
        return remoteSchema;
    }

    @JsonProperty
    public String getRemoteTable()
    {
        return remoteTable;
    }

    @JsonProperty
    public String getMapping()
    {
        return mapping;
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
        TableMappingRule that = (TableMappingRule) o;
        return remoteSchema.equals(that.remoteSchema) && remoteTable.equals(that.remoteTable) && mapping.equals(that.mapping);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteSchema, remoteTable, mapping);
    }
}

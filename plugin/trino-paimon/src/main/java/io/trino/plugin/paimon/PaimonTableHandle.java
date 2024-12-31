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
package io.trino.plugin.paimon;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PaimonTableHandle
        implements ConnectorTableHandle, ConnectorInsertTableHandle, ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<PaimonColumnHandle> predicate;

    @JsonCreator
    public PaimonTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("predicate") TupleDomain<PaimonColumnHandle> predicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<PaimonColumnHandle> getPredicate()
    {
        return predicate;
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
        PaimonTableHandle that = (PaimonTableHandle) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, predicate);
    }
}

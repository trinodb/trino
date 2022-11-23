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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class TableInfo
{
    private final Optional<String> connectorName;
    private final QualifiedObjectName tableName;
    private final TupleDomain<ColumnHandle> predicate;

    @JsonCreator
    public TableInfo(
            @JsonProperty("connectorName") Optional<String> connectorName,
            @JsonProperty("tableName") QualifiedObjectName tableName,
            @JsonProperty("predicate") TupleDomain<ColumnHandle> predicate)
    {
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public Optional<String> getConnectorName()
    {
        return connectorName;
    }

    @JsonProperty
    public QualifiedObjectName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPredicate()
    {
        return predicate;
    }
}

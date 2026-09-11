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
package io.trino.spi.function.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableMetadata;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * This class represents a table argument passed to a Table Function whose specification has
 * {@link TableArgumentSpecification#isUseTableMetadata()} set. Unlike {@link TableArgument}, it
 * carries the referenced table's {@link ConnectorTableMetadata} instead of row data: the engine
 * does not plan a source or read any rows for this argument.
 * <p>
 * This argument is not JSON-serializable: {@link ConnectorTableMetadata}'s properties are
 * connector-defined and not generically JSON-safe. It is only ever produced and consumed on the
 * coordinator during analysis, before query fragments are distributed to workers.
 */
public class TableMetadataArgument
        extends Argument
{
    private final ConnectorTableMetadata tableMetadata;

    @JsonCreator
    public TableMetadataArgument(@JsonProperty("tableMetadata") ConnectorTableMetadata tableMetadata)
    {
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
    }

    @JsonProperty
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
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
        TableMetadataArgument that = (TableMetadataArgument) o;
        return tableMetadata.equals(that.tableMetadata);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableMetadata);
    }
}

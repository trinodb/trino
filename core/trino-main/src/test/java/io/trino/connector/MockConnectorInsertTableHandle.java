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
package io.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MockConnectorInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final SchemaTableName tableName;

    @JsonCreator
    public MockConnectorInsertTableHandle(@JsonProperty("tableName") SchemaTableName tableName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
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
        MockConnectorInsertTableHandle other = (MockConnectorInsertTableHandle) o;
        return Objects.equals(tableName, other.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName);
    }
}

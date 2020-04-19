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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.type.Type;

import java.util.List;

public class ElasticsearchOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String schema;
    private final String index;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    @JsonCreator
    public ElasticsearchOutputTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes)
    {
        this.schema = schema;
        this.index = index;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }
}

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
package io.trino.plugin.google.sheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SheetsConnectorInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String tableName;
    private final List<SheetsColumnHandle> columns;

    @JsonCreator
    public SheetsConnectorInsertTableHandle(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<SheetsColumnHandle> columns)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<SheetsColumnHandle> getColumns()
    {
        return columns;
    }
}

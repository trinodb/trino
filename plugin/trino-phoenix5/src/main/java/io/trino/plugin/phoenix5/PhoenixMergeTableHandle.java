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
package io.trino.plugin.phoenix5;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class PhoenixMergeTableHandle
        extends JdbcMergeTableHandle
{
    @JsonCreator
    public PhoenixMergeTableHandle(
            @JsonProperty("tableHandle") JdbcTableHandle tableHandle,
            @JsonProperty("outputTableHandle") PhoenixOutputTableHandle phoenixOutputTableHandle,
            @JsonProperty("primaryKeys") List<JdbcColumnHandle> primaryKeys,
            @JsonProperty("dataColumns") List<JdbcColumnHandle> dataColumns,
            @JsonProperty("updateCaseColumns") Map<Integer, Collection<ColumnHandle>> updateCaseColumns)
    {
        super(tableHandle, phoenixOutputTableHandle, primaryKeys, dataColumns, updateCaseColumns);
    }
}

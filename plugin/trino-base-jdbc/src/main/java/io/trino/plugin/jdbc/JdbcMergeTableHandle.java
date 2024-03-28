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
package io.trino.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorMergeTableHandle;

import java.util.Optional;

public class JdbcMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final JdbcTableHandle tableHandle;
    private final JdbcOutputTableHandle outputTableHandle;
    private final Optional<JdbcOutputTableHandle> deleteTableHandle;
    private final JdbcColumnHandle mergeRowIdColumnHandle;

    public JdbcMergeTableHandle(
            JdbcTableHandle tableHandle,
            JdbcOutputTableHandle outputTableHandle,
            JdbcColumnHandle mergeRowIdColumnHandle)
    {
        this.tableHandle = tableHandle;
        this.outputTableHandle = outputTableHandle;
        this.deleteTableHandle = Optional.empty();
        this.mergeRowIdColumnHandle = mergeRowIdColumnHandle;
    }

    @JsonCreator
    public JdbcMergeTableHandle(
            @JsonProperty("tableHandle") JdbcTableHandle tableHandle,
            @JsonProperty("outputTableHandle") JdbcOutputTableHandle outputTableHandle,
            @JsonProperty("deleteTableHandle") Optional<JdbcOutputTableHandle> deleteTableHandle,
            @JsonProperty("mergeRowIdColumnHandle") JdbcColumnHandle mergeRowIdColumnHandle)
    {
        this.tableHandle = tableHandle;
        this.outputTableHandle = outputTableHandle;
        this.deleteTableHandle = deleteTableHandle;
        this.mergeRowIdColumnHandle = mergeRowIdColumnHandle;
    }

    @JsonProperty
    @Override
    public JdbcTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public JdbcOutputTableHandle getOutputTableHandle()
    {
        return outputTableHandle;
    }

    @JsonProperty
    public Optional<JdbcOutputTableHandle> getDeleteTableHandle()
    {
        return deleteTableHandle;
    }

    @JsonProperty
    public JdbcColumnHandle mergeRowIdColumnHandle()
    {
        return mergeRowIdColumnHandle;
    }
}

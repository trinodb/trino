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
package io.trino.plugin.ignite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IgniteOutputTableHandle
        extends JdbcOutputTableHandle
{
    private final Optional<String> dummyIdColumn;

    @JsonCreator
    public IgniteOutputTableHandle(
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("jdbcColumnTypes") Optional<List<JdbcTypeHandle>> jdbcColumnTypes,
            @JsonProperty("dummyIdColumn") Optional<String> dummyIdColumn)
    {
        super(remoteTableName, columnNames, columnTypes, jdbcColumnTypes, Optional.empty(), Optional.empty());
        this.dummyIdColumn = requireNonNull(dummyIdColumn, "dummyIdColumn is null");
    }

    @JsonProperty
    public Optional<String> dummyIdColumn()
    {
        return dummyIdColumn;
    }
}

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
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JdbcOutputTableHandle
        implements ConnectorOutputTableHandle, ConnectorInsertTableHandle
{
    private final RemoteTableName remoteTableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final Optional<List<JdbcTypeHandle>> jdbcColumnTypes;
    private final Optional<String> temporaryTableName;
    private final Optional<String> pageSinkIdColumnName;

    @JsonCreator
    public JdbcOutputTableHandle(
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("jdbcColumnTypes") Optional<List<JdbcTypeHandle>> jdbcColumnTypes,
            @JsonProperty("temporaryTableName") Optional<String> temporaryTableName,
            @JsonProperty("pageSinkIdColumnName") Optional<String> pageSinkIdColumnName)
    {
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.temporaryTableName = requireNonNull(temporaryTableName, "temporaryTableName is null");

        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        jdbcColumnTypes.ifPresent(jdbcTypeHandles -> checkArgument(jdbcTypeHandles.size() == columnNames.size(), "columnNames and jdbcColumnTypes sizes don't match"));
        this.jdbcColumnTypes = jdbcColumnTypes.map(ImmutableList::copyOf);
        this.pageSinkIdColumnName = requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
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

    @JsonProperty
    public Optional<List<JdbcTypeHandle>> getJdbcColumnTypes()
    {
        return jdbcColumnTypes;
    }

    @JsonProperty
    public Optional<String> getTemporaryTableName()
    {
        return temporaryTableName;
    }

    @JsonProperty
    public Optional<String> getPageSinkIdColumnName()
    {
        return pageSinkIdColumnName;
    }

    @Override
    public String toString()
    {
        return "jdbc:%s".formatted(remoteTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                remoteTableName,
                columnNames,
                columnTypes,
                jdbcColumnTypes,
                temporaryTableName,
                pageSinkIdColumnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JdbcOutputTableHandle other = (JdbcOutputTableHandle) obj;
        return Objects.equals(this.remoteTableName, other.remoteTableName) &&
                Objects.equals(this.columnNames, other.columnNames) &&
                Objects.equals(this.columnTypes, other.columnTypes) &&
                Objects.equals(this.jdbcColumnTypes, other.jdbcColumnTypes) &&
                Objects.equals(this.temporaryTableName, other.temporaryTableName) &&
                Objects.equals(this.pageSinkIdColumnName, other.pageSinkIdColumnName);
    }
}

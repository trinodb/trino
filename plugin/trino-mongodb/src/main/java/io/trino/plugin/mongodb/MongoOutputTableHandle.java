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
package io.trino.plugin.mongodb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MongoOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final RemoteTableName remoteTableName;
    private final List<MongoColumnHandle> columns;
    private final Optional<String> temporaryTableName;
    private final Optional<String> pageSinkIdColumnName;

    @JsonCreator
    public MongoOutputTableHandle(
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("columns") List<MongoColumnHandle> columns,
            @JsonProperty("temporaryTableName") Optional<String> temporaryTableName,
            @JsonProperty("pageSinkIdColumnName") Optional<String> pageSinkIdColumnName)
    {
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.temporaryTableName = requireNonNull(temporaryTableName, "temporaryTableName is null");
        this.pageSinkIdColumnName = requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
        checkArgument(temporaryTableName.isPresent() == pageSinkIdColumnName.isPresent(),
                "temporaryTableName.isPresent is not equal to pageSinkIdColumnName.isPresent");
    }

    @JsonProperty
    public RemoteTableName getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public List<MongoColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getTemporaryTableName()
    {
        return temporaryTableName;
    }

    public Optional<RemoteTableName> getTemporaryRemoteTableName()
    {
        return temporaryTableName.map(tableName -> new RemoteTableName(remoteTableName.getDatabaseName(), tableName));
    }

    @JsonProperty
    public Optional<String> getPageSinkIdColumnName()
    {
        return pageSinkIdColumnName;
    }
}

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record MongoInsertTableHandle(
        RemoteTableName remoteTableName,
        List<MongoColumnHandle> columns,
        Optional<String> temporaryTableName,
        Optional<String> pageSinkIdColumnName)
        implements ConnectorInsertTableHandle
{
    public MongoInsertTableHandle
    {
        requireNonNull(remoteTableName, "remoteTableName is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        requireNonNull(temporaryTableName, "temporaryTableName is null");
        requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
        checkArgument(temporaryTableName.isPresent() == pageSinkIdColumnName.isPresent(),
                "temporaryTableName.isPresent is not equal to pageSinkIdColumnName.isPresent");
    }

    @JsonIgnore
    public Optional<RemoteTableName> getTemporaryRemoteTableName()
    {
        return temporaryTableName.map(tableName -> new RemoteTableName(remoteTableName.databaseName(), tableName));
    }
}

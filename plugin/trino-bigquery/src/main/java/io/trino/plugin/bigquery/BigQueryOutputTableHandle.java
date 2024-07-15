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
package io.trino.plugin.bigquery;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record BigQueryOutputTableHandle(
        RemoteTableName remoteTableName,
        List<String> columnNames,
        List<Type> columnTypes,
        Optional<String> temporaryTableName,
        Optional<String> pageSinkIdColumnName)
        implements ConnectorOutputTableHandle
{
    public BigQueryOutputTableHandle
    {
        requireNonNull(remoteTableName, "remoteTableName is null");
        columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must have the same size");
        requireNonNull(temporaryTableName, "temporaryTableName is null");
        requireNonNull(pageSinkIdColumnName, "pageSinkIdColumnName is null");
        checkArgument(temporaryTableName.isPresent() == pageSinkIdColumnName.isPresent(),
                "temporaryTableName.isPresent is not equal to pageSinkIdColumn.isPresent");
    }

    @JsonIgnore
    public Optional<RemoteTableName> getTemporaryRemoteTableName()
    {
        return temporaryTableName.map(tableName -> new RemoteTableName(remoteTableName.projectId(), remoteTableName.datasetName(), tableName));
    }
}

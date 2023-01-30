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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BigQueryInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final RemoteTableName remoteTableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String temporaryTableName;
    private final String pageSinkIdColumnName;

    @JsonCreator
    public BigQueryInsertTableHandle(
            @JsonProperty("remoteTableName") RemoteTableName remoteTableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("temporaryTableName") String temporaryTableName,
            @JsonProperty("pageSinkIdColumnName") String pageSinkIdColumnName)
    {
        this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must have the same size");
        this.temporaryTableName = requireNonNull(temporaryTableName, "temporaryTableName is null");
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
    public String getTemporaryTableName()
    {
        return temporaryTableName;
    }

    public RemoteTableName getTemporaryRemoteTableName()
    {
        return new RemoteTableName(remoteTableName.getProjectId(), remoteTableName.getDatasetName(), temporaryTableName);
    }

    @JsonProperty
    public String getPageSinkIdColumnName()
    {
        return pageSinkIdColumnName;
    }
}

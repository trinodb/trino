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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class RemoteTableName
{
    private final String projectId;
    private final String datasetName;
    private final String tableName;

    @JsonCreator
    public RemoteTableName(String projectId, String datasetName, String tableName)
    {
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.datasetName = requireNonNull(datasetName, "datasetName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public RemoteTableName(TableId tableId)
    {
        this(tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    public TableId toTableId()
    {
        return TableId.of(projectId, datasetName, tableName);
    }

    public TableName toTableName()
    {
        return TableName.of(projectId, datasetName, tableName);
    }

    @JsonProperty
    public String getProjectId()
    {
        return projectId;
    }

    @JsonProperty
    public String getDatasetName()
    {
        return datasetName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteTableName that = (RemoteTableName) o;
        return projectId.equals(that.projectId) &&
                datasetName.equals(that.datasetName) &&
                tableName.equals(that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(projectId, datasetName, tableName);
    }

    @Override
    public String toString()
    {
        return format("%s.%s.%s", projectId, datasetName, tableName);
    }
}

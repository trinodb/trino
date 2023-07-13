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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.cassandra.util.CassandraCqlUtils;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CassandraTable
{
    private final CassandraNamedRelationHandle tableHandle;
    private final List<CassandraColumnHandle> columns;

    public CassandraTable(CassandraNamedRelationHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public List<CassandraColumnHandle> getColumns()
    {
        return columns;
    }

    public CassandraNamedRelationHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<CassandraColumnHandle> getPartitionKeyColumns()
    {
        return columns.stream()
                .filter(CassandraColumnHandle::isPartitionKey)
                .collect(toImmutableList());
    }

    public List<CassandraColumnHandle> getClusteringKeyColumns()
    {
        return columns.stream()
                .filter(CassandraColumnHandle::isClusteringKey)
                .collect(toImmutableList());
    }

    public String getTokenExpression()
    {
        StringBuilder sb = new StringBuilder();
        for (CassandraColumnHandle column : getPartitionKeyColumns()) {
            if (sb.length() == 0) {
                sb.append("token(");
            }
            else {
                sb.append(",");
            }
            sb.append(CassandraCqlUtils.validColumnName(column.getName()));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CassandraTable)) {
            return false;
        }
        CassandraTable that = (CassandraTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .toString();
    }
}

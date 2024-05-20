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

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public record CassandraTable(CassandraNamedRelationHandle tableHandle, List<CassandraColumnHandle> columns)
{
    public CassandraTable
    {
        requireNonNull(tableHandle, "tableHandle is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    }

    public List<CassandraColumnHandle> partitionKeyColumns()
    {
        return columns.stream()
                .filter(CassandraColumnHandle::partitionKey)
                .collect(toImmutableList());
    }

    public List<CassandraColumnHandle> clusteringKeyColumns()
    {
        return columns.stream()
                .filter(CassandraColumnHandle::clusteringKey)
                .collect(toImmutableList());
    }

    public String tokenExpression()
    {
        StringBuilder sb = new StringBuilder();
        for (CassandraColumnHandle column : partitionKeyColumns()) {
            if (sb.length() == 0) {
                sb.append("token(");
            }
            else {
                sb.append(",");
            }
            sb.append(CassandraCqlUtils.validColumnName(column.name()));
        }
        sb.append(")");
        return sb.toString();
    }
}

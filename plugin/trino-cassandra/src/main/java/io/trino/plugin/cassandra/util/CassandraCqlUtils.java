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
package io.trino.plugin.cassandra.util;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.cassandra.CassandraColumnHandle;
import io.trino.plugin.cassandra.CassandraNamedRelationHandle;
import io.trino.plugin.cassandra.CassandraPartition;
import io.trino.spi.connector.ColumnHandle;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.oss.driver.internal.core.util.Strings.doubleQuote;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CassandraCqlUtils
{
    private CassandraCqlUtils() {}

    public static final String ID_COLUMN_NAME = "id";
    public static final String EMPTY_COLUMN_NAME = "__empty__";

    public static String validSchemaName(String identifier)
    {
        return doubleQuote(identifier);
    }

    public static String validTableName(String identifier)
    {
        return doubleQuote(identifier);
    }

    public static String validColumnName(String identifier)
    {
        if (identifier.isEmpty() || identifier.equals(EMPTY_COLUMN_NAME)) {
            return "\"\"";
        }

        return doubleQuote(identifier);
    }

    public static String quoteStringLiteral(String string)
    {
        return "'" + string.replace("'", "''") + "'";
    }

    public static String quoteStringLiteralForJson(String string)
    {
        return '"' + new String(JsonStringEncoder.getInstance().quoteAsUTF8(string), UTF_8) + '"';
    }

    public static void appendSelectColumns(StringBuilder stringBuilder, List<? extends ColumnHandle> columns)
    {
        appendSelectColumns(stringBuilder, columns, true);
    }

    private static void appendSelectColumns(StringBuilder stringBuilder, List<? extends ColumnHandle> columns, boolean first)
    {
        for (ColumnHandle column : columns) {
            if (first) {
                first = false;
            }
            else {
                stringBuilder.append(",");
            }
            stringBuilder.append(validColumnName(((CassandraColumnHandle) column).getName()));
        }
    }

    public static String cqlNameToSqlName(String name)
    {
        if (name.isEmpty()) {
            return EMPTY_COLUMN_NAME;
        }
        return name;
    }

    public static String sqlNameToCqlName(String name)
    {
        if (name.equals(EMPTY_COLUMN_NAME)) {
            return "";
        }
        return name;
    }

    public static List<String> selection(List<CassandraColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> validColumnName(column.getName()))
                .collect(toImmutableList());
    }

    public static Select selectFrom(CassandraNamedRelationHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        SelectFrom selectFrom = from(tableHandle);
        return columns.isEmpty() ? selectFrom.all() : selectFrom.columns(selection(columns));
    }

    public static SelectFrom from(CassandraNamedRelationHandle tableHandle)
    {
        String schema = validSchemaName(tableHandle.getSchemaName());
        String table = validTableName(tableHandle.getTableName());
        return QueryBuilder.selectFrom(schema, table);
    }

    public static Select selectDistinctFrom(CassandraNamedRelationHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        SelectFrom selectFrom = from(tableHandle).distinct();
        if (columns.isEmpty()) {
            return selectFrom.all();
        }
        return selectFrom.columns(selection(columns));
    }

    private static String getWhereCondition(String partition, String clusteringKeyPredicates)
    {
        List<String> conditions = new ArrayList<>();
        conditions.add(partition);
        if (!clusteringKeyPredicates.isEmpty()) {
            conditions.add(clusteringKeyPredicates);
        }
        return String.join(" AND ", conditions);
    }

    private static String deleteFrom(String schemaName, String tableName, CassandraPartition partition, String clusteringKeyPredicates)
    {
        return format("DELETE FROM \"%s\".\"%s\" WHERE %s",
                schemaName, tableName, getWhereCondition(partition.getPartitionId(), clusteringKeyPredicates));
    }

    public static List<String> getDeleteQueries(CassandraNamedRelationHandle handle)
    {
        ImmutableList.Builder<String> queries = ImmutableList.builder();
        for (CassandraPartition partition : handle.getPartitions().orElse(ImmutableList.of())) {
            queries.add(deleteFrom(handle.getSchemaName(), handle.getTableName(), partition, handle.getClusteringKeyPredicates()));
        }
        return queries.build();
    }
}

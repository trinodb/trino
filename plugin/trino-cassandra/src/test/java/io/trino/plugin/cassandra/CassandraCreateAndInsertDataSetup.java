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
import io.airlift.json.JsonCodec;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TestTable;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.cassandra.CassandraMetadata.PRESTO_COMMENT_METADATA;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.ID_COLUMN_NAME;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

// The reasons for not using CreateAndInsertDataSetup are:
// (1) Cassandra tables must define a single PRIMARY KEY
// (2) CQL requires that INSERT INTO statements must provide a list of the columns to be inserted
public class CassandraCreateAndInsertDataSetup
        implements DataSetup
{
    private static final JsonCodec<List<ExtraColumnMetadata>> LIST_EXTRA_COLUMN_METADATA_CODEC = JsonCodec.listJsonCodec(ExtraColumnMetadata.class);

    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;
    private final String keyspaceName;
    private final CassandraServer cassandraServer;

    public CassandraCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix, CassandraServer cassandraServer)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
        keyspaceName = verifyTableNamePrefixAndGetKeyspaceName(tableNamePrefix);
        this.cassandraServer = requireNonNull(cassandraServer, "cassandraServer is null");
    }

    private static String verifyTableNamePrefixAndGetKeyspaceName(String tableNamePrefix)
    {
        String[] keyspaceNameAndTableName = tableNamePrefix.split("\\.");
        verify(keyspaceNameAndTableName.length == 2, "Invalid tableNamePrefix: %s", tableNamePrefix);
        return keyspaceNameAndTableName[0];
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestTable testTable = createTestTable(inputs);
        String tableName = testTable.getName().substring(keyspaceName.length() + 1);
        try {
            insertRows(keyspaceName, tableName, inputs);
            refreshSizeEstimates(keyspaceName, tableName);
        }
        catch (Exception e) {
            closeAllSuppress(e, testTable);
            throw e;
        }
        return testTable;
    }

    private void insertRows(String keyspaceName, String tableName, List<ColumnSetup> inputs)
    {
        String columnNames = IntStream.range(0, inputs.size())
                .mapToObj(column -> format("col_%d", column))
                .collect(joining(", ", ID_COLUMN_NAME + ", ", ""));
        String valueLiterals = inputs.stream()
                .map(ColumnSetup::getInputLiteral)
                .collect(joining(", ", "00000000-0000-0000-0000-000000000000, ", ""));
        sqlExecutor.execute(format("INSERT INTO %s.%s (%s) VALUES(%s)", keyspaceName, tableName, columnNames, valueLiterals));
    }

    private void refreshSizeEstimates(String keyspaceName, String tableName)
    {
        try {
            cassandraServer.refreshSizeEstimates(keyspaceName, tableName);
        }
        catch (Exception e) {
            throw new RuntimeException(format("Error refreshing size estimates for %s.%s", keyspaceName, tableName), e);
        }
    }

    private TestTable createTestTable(List<ColumnSetup> inputs)
    {
        return new TestTable(sqlExecutor, tableNamePrefix, tableDefinition(inputs));
    }

    private String tableDefinition(List<ColumnSetup> inputs)
    {
        checkState(inputs.stream().allMatch(input -> input.getDeclaredType().isPresent()), "Explicit declared input types are required when creating a table directly from Cassandra");

        ImmutableList.Builder<ExtraColumnMetadata> columnExtra = ImmutableList.builder();
        columnExtra.add(new ExtraColumnMetadata(ID_COLUMN_NAME, true));
        IntStream.range(0, inputs.size())
                .forEach(column -> columnExtra.add(new ExtraColumnMetadata(format("col_%d", column), false)));
        String columnMetadata = LIST_EXTRA_COLUMN_METADATA_CODEC.toJson(columnExtra.build());

        return IntStream.range(0, inputs.size())
                .mapToObj(column -> format("col_%d %s", column, inputs.get(column).getDeclaredType().orElseThrow()))
                .collect(joining(",", "(" + ID_COLUMN_NAME + " uuid PRIMARY KEY,", ") WITH comment=" + quoteStringLiteral(PRESTO_COMMENT_METADATA + " " + columnMetadata)));
    }
}

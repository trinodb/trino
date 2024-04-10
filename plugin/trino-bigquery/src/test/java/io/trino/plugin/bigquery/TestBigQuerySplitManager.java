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

import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Optional;

import static com.google.cloud.bigquery.Field.Mode.REQUIRED;
import static com.google.cloud.bigquery.StandardSQLTypeName.INT64;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bigquery.BigQueryFilterQueryBuilder.buildFilter;
import static io.trino.plugin.bigquery.ViewMaterializationCache.TEMP_TABLE_PREFIX;
import static io.trino.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBigQuerySplitManager
{
    private final Connector connector;
    private final BigQuerySqlExecutor bigQueryExecutor;

    public TestBigQuerySplitManager()
    {
        BigQueryConnectorFactory connectorFactory = new BigQueryConnectorFactory();
        connector = connectorFactory.create("bigquery", ImmutableMap.of("bigquery.views-enabled", "true"), new TestingConnectorContext());
        bigQueryExecutor = new BigQuerySqlExecutor();
    }

    @AfterAll
    public void tearDown()
    {
        connector.shutdown();
    }

    @Test
    void testBigQueryMaterializedView()
    {
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(new BigQuerySessionProperties(new BigQueryConfig()).getSessionProperties())
                .build();
        ConnectorTransactionHandle transaction = connector.beginTransaction(READ_UNCOMMITTED, false, true);
        ConnectorMetadata metadata = connector.getMetadata(session, transaction);

        String materializedView = "test_materialized_view" + randomNameSuffix();
        onBigQuery("CREATE MATERIALIZED VIEW test." + materializedView + " AS SELECT count(1) AS cnt FROM tpch.region");
        try {
            BigQueryTableHandle table = (BigQueryTableHandle) metadata.getTableHandle(session, new SchemaTableName("test", materializedView), Optional.empty(), Optional.empty());

            ReadSession readSession = createReadSession(session, table);
            assertThat(readSession.getTable()).contains(TEMP_TABLE_PREFIX);

            // Ignore constraints when creating temporary tables by default (view_materialization_with_filter is false)
            BigQueryColumnHandle column = new BigQueryColumnHandle("cnt", BIGINT, INT64, true, REQUIRED, ImmutableList.of(), null, false);
            BigQueryTableHandle tableDifferentFilter = new BigQueryTableHandle(table.relationHandle(), TupleDomain.fromFixedValues(ImmutableMap.of(column, new NullableValue(BIGINT, 0L))), table.projectedColumns());
            assertThat(createReadSession(session, tableDifferentFilter).getTable())
                    .isEqualTo(readSession.getTable());

            // Don't reuse the same temporary table when view_materialization_with_filter is true
            ConnectorSession viewMaterializationWithFilter = TestingConnectorSession.builder()
                    .setPropertyMetadata(new BigQuerySessionProperties(new BigQueryConfig()).getSessionProperties())
                    .setPropertyValues(ImmutableMap.of("view_materialization_with_filter", true))
                    .build();
            String temporaryTableWithFilter = createReadSession(viewMaterializationWithFilter, tableDifferentFilter).getTable();
            assertThat(temporaryTableWithFilter)
                    .isNotEqualTo(readSession.getTable());

            // Reuse the same temporary table when the filters are identical
            assertThat(createReadSession(viewMaterializationWithFilter, tableDifferentFilter).getTable())
                    .isEqualTo(temporaryTableWithFilter);
        }
        finally {
            onBigQuery("DROP MATERIALIZED VIEW test." + materializedView);
        }
    }

    private ReadSession createReadSession(ConnectorSession session, BigQueryTableHandle table)
    {
        BigQuerySplitManager splitManager = (BigQuerySplitManager) connector.getSplitManager();
        return splitManager.createReadSession(
                session,
                table.asPlainTable().getRemoteTableName().toTableId(),
                table.projectedColumns().orElseThrow().stream()
                        .map(BigQueryColumnHandle::name)
                        .collect(toImmutableList()),
                buildFilter(table.constraint()),
                1);
    }

    private void onBigQuery(@Language("SQL") String sql)
    {
        bigQueryExecutor.execute(sql);
    }
}

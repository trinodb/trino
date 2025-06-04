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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.cloud.bigquery.BigQuery.DatasetDeleteOption.deleteContents;
import static com.google.cloud.bigquery.BigQuery.DatasetListOption.labelFilter;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class BigQueryQueryRunner
{
    static final String BIGQUERY_CREDENTIALS_KEY = requiredNonEmptySystemProperty("testing.bigquery.credentials-key");
    public static final String TPCH_SCHEMA = "tpch";
    public static final String TEST_SCHEMA = "test";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("com.google.cloud.bigquery.storage", Level.OFF);
    }

    private BigQueryQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Map<String, String> connectorProperties = ImmutableMap.of();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("bigquery")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder setConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = connectorProperties;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                // note: additional copy via ImmutableList so that if fails on nulls
                Map<String, String> connectorProperties = new HashMap<>(ImmutableMap.copyOf(this.connectorProperties));
                connectorProperties.putIfAbsent("bigquery.credentials-key", BIGQUERY_CREDENTIALS_KEY);
                connectorProperties.putIfAbsent("bigquery.views-enabled", "true");
                connectorProperties.putIfAbsent("bigquery.view-expire-duration", "30m");
                connectorProperties.putIfAbsent("bigquery.rpc-retries", "10");
                connectorProperties.putIfAbsent("bigquery.rpc-retry-delay", "200ms");
                connectorProperties.putIfAbsent("bigquery.rpc-retry-delay-multiplier", "1.5");
                connectorProperties.putIfAbsent("bigquery.rpc-timeout", "30s");

                queryRunner.installPlugin(new BigQueryPlugin());
                queryRunner.createCatalog("bigquery", "bigquery", connectorProperties);

                queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TPCH_SCHEMA);
                queryRunner.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static class BigQuerySqlExecutor
            implements SqlExecutor
    {
        private static final Map.Entry<String, String> BIG_QUERY_SQL_EXECUTOR_LABEL = Maps.immutableEntry("ci-automation-source", "trino_tests_big_query_sql_executor");

        private final BigQuery bigQuery;

        public BigQuerySqlExecutor()
        {
            this.bigQuery = createBigQueryClient();
        }

        @Override
        public void execute(@Language("SQL") String sql)
        {
            executeQuery(sql);
        }

        public TableResult executeQuery(@Language("SQL") String sql)
        {
            try {
                return bigQuery.query(QueryJobConfiguration.of(sql));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        public void createDataset(String datasetName)
        {
            DatasetInfo dataset = DatasetInfo.newBuilder(datasetName)
                    .setLabels(ImmutableMap.copyOf(ImmutableSet.of(BIG_QUERY_SQL_EXECUTOR_LABEL)))
                    .build();
            bigQuery.create(dataset);
        }

        public void dropDatasetIfExists(String dataset)
        {
            bigQuery.delete(dataset, deleteContents());
        }

        public List<String> getSelfCreatedDatasets()
        {
            ImmutableList.Builder<String> datasetNames = ImmutableList.builder();
            for (Dataset dataset : bigQuery.listDatasets(
                    labelFilter(format("labels.%s:%s",
                            BIG_QUERY_SQL_EXECUTOR_LABEL.getKey(),
                            BIG_QUERY_SQL_EXECUTOR_LABEL.getValue()))).iterateAll()) {
                datasetNames.add(dataset.getDatasetId().getDataset());
            }
            return datasetNames.build();
        }

        public List<String> getTableNames(String dataset)
        {
            ImmutableList.Builder<String> tableNames = ImmutableList.builder();
            for (Table table : bigQuery.listTables(DatasetId.of(dataset)).iterateAll()) {
                tableNames.add(table.getTableId().getTable());
            }
            return tableNames.build();
        }

        public BigQuery getBigQuery()
        {
            return bigQuery;
        }

        private static BigQuery createBigQueryClient()
        {
            try {
                InputStream jsonKey = new ByteArrayInputStream(Base64.getDecoder().decode(BIGQUERY_CREDENTIALS_KEY));
                return BigQueryOptions.newBuilder()
                        .setCredentials(ServiceAccountCredentials.fromStream(jsonKey))
                        .build()
                        .getService();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = BigQueryQueryRunner.builder()
                .setCoordinatorProperties(Map.of("http-server.http.port", "8080"))
                .setInitialTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(BigQueryQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

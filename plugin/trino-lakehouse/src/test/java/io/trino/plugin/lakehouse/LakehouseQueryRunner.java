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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.util.Set;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.lakehouse.TableType.DELTA;
import static io.trino.plugin.lakehouse.TableType.HIVE;
import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;

public final class LakehouseQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.iceberg", Level.OFF);
    }

    private LakehouseQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final ImmutableMap.Builder<String, String> lakehouseProperties = ImmutableMap.builder();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("lakehouse")
                    .setSchema("tpch")
                    .build());
        }

        public Builder addLakehouseProperty(String key, String value)
        {
            lakehouseProperties.put(key, value);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                // needed for $iceberg_theta_stat function
                queryRunner.installPlugin(new IcebergPlugin());

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new TpcdsPlugin());
                queryRunner.createCatalog("tpcds", "tpcds");

                queryRunner.installPlugin(new LakehousePlugin());
                queryRunner.createCatalog("lakehouse", "lakehouse", lakehouseProperties.buildOrThrow());

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        File metastoreDir = createTempDirectory("delta_query_runner").toFile();
        metastoreDir.deleteOnExit();

        @SuppressWarnings("resource")
        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addLakehouseProperty("hive.metastore", "file")
                .addLakehouseProperty("hive.metastore.catalog.dir", metastoreDir.toURI().toString())
                .addLakehouseProperty("fs.hadoop.enabled", "true")
                .build();

        for (TableType tableType : Set.of(HIVE, ICEBERG, DELTA)) {
            String type = tableType.name().toLowerCase(ENGLISH);
            queryRunner.execute("CREATE SCHEMA " + type);
            for (TpchTable<?> table : TpchTable.getTables()) {
                queryRunner.execute("CREATE TABLE " + type + "." + table.getTableName() + " WITH (type = '" + type + "')" +
                        "AS SELECT * FROM tpch.tiny." + table.getTableName());
            }
        }

        Logger log = Logger.get(LakehouseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

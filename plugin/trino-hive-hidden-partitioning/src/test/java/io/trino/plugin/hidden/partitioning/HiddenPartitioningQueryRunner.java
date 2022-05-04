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
package io.trino.plugin.hidden.partitioning;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.log.Level.WARN;
import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public final class HiddenPartitioningQueryRunner
{
    private static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    private HiddenPartitioningQueryRunner() {}

    public static class Builder
            extends DistributedQueryRunner.Builder
    {
        private Map<String, String> hiveProperties = ImmutableMap.of();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private Function<DistributedQueryRunner, HiveMetastore> metastore = queryRunner -> {
            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
            return new FileHiveMetastore(
                    new NodeVersion("test_version"),
                    HDFS_ENVIRONMENT,
                    new MetastoreConfig(),
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory(baseDir.toURI().toString())
                            .setMetastoreUser("test"));
        };
        private Module module = EMPTY_MODULE;

        protected Builder()
        {
            super(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
        }

        public Builder setHiveProperties(Map<String, String> hiveProperties)
        {
            this.hiveProperties = ImmutableMap.copyOf(requireNonNull(hiveProperties, "hiveProperties is null"));
            return this;
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        public Builder setMetastore(Function<DistributedQueryRunner, HiveMetastore> metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
            return this;
        }

        public Builder setModule(Module module)
        {
            this.module = requireNonNull(module, "module is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
            setupLogging();

            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                HiveMetastore metastore = this.metastore.apply(queryRunner);
                queryRunner.installPlugin(new TestingHiddenPartitioningPlugin(metastore, module));

                Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                        .put("hive.orc.time-zone", TIME_ZONE.getID())
                        .put("hive.max-partitions-per-scan", "1000")
                        .buildOrThrow();

                hiveProperties = new HashMap<>(hiveProperties);
                hiveProperties.putAll(this.hiveProperties);
                hiveProperties.putIfAbsent("hive.security", "sql-standard");

                Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                        .putAll(hiveProperties)
                        .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                        .put("hive.compression-codec", "NONE") // so that the file is splittable
                        .buildOrThrow();
                queryRunner.createCatalog(HIVE_CATALOG, "test-hidden-partitioning", hiveProperties);

                if (!initialTables.isEmpty()) {
                    populateData(queryRunner, metastore);
                }

                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }

        private void populateData(DistributedQueryRunner queryRunner, HiveMetastore metastore)
        {
            if (!metastore.getDatabase(TPCH_SCHEMA).isPresent()) {
                metastore.createDatabase(createDatabaseMetastoreObject(TPCH_SCHEMA));
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(Optional.empty()), initialTables);
            }
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.parquet.hadoop", WARN);
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    private static Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withRoles(role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of()))
                        .build())
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }
}

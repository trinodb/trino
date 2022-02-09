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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class IcebergQueryRunner
{
    private static final Logger log = Logger.get(IcebergQueryRunner.class);

    public static final String ICEBERG_CATALOG = "iceberg";

    private IcebergQueryRunner() {}

    public static DistributedQueryRunner createIcebergQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createIcebergQueryRunner(
                ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                tables);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createIcebergQueryRunner(
                extraProperties,
                connectorProperties,
                tables,
                Optional.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Optional<File> metastoreDirectory)
            throws Exception
    {
        return createIcebergQueryRunner(
                extraProperties,
                connectorProperties,
                SchemaInitializer.builder()
                        .withClonedTpchTables(tables)
                        .build(),
                metastoreDirectory);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            SchemaInitializer schemaInitializer,
            Optional<File> metastoreDirectory)
            throws Exception
    {
        Builder builder = builder()
                .setExtraProperties(extraProperties)
                .setIcebergProperties(connectorProperties)
                .setSchemaInitializer(schemaInitializer);

        metastoreDirectory.ifPresent(builder::setMetastoreDirectory);
        return builder.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private Optional<File> metastoreDirectory = Optional.empty();
        private ImmutableMap.Builder<String, String> icebergProperties = ImmutableMap.builder();
        private Optional<SchemaInitializer> schemaInitializer = Optional.empty();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(ICEBERG_CATALOG)
                    .build());
        }

        public Builder setMetastoreDirectory(File metastoreDirectory)
        {
            this.metastoreDirectory = Optional.of(metastoreDirectory);
            return self();
        }

        public Builder setIcebergProperties(Map<String, String> icebergProperties)
        {
            this.icebergProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(icebergProperties, "icebergProperties is null"));
            return self();
        }

        public Builder addIcebergProperty(String key, String value)
        {
            this.icebergProperties.put(key, value);
            return self();
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            setSchemaInitializer(SchemaInitializer.builder().withClonedTpchTables(initialTables).build());
            return self();
        }

        public Builder setSchemaInitializer(SchemaInitializer schemaInitializer)
        {
            checkState(this.schemaInitializer.isEmpty(), "schemaInitializer is already set");
            this.schemaInitializer = Optional.of(requireNonNull(schemaInitializer, "schemaInitializer is null"));
            amendSession(sessionBuilder -> sessionBuilder.setSchema(schemaInitializer.getSchemaName()));
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));

                queryRunner.installPlugin(new IcebergPlugin());
                Map<String, String> icebergProperties = new HashMap<>();
                icebergProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
                icebergProperties.put("hive.metastore.catalog.dir", dataDir.toString());
                icebergProperties.putAll(this.icebergProperties.buildOrThrow());

                queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);
                schemaInitializer.orElse(SchemaInitializer.builder().build()).accept(queryRunner);

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
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = createIcebergQueryRunner(properties, Map.of(), TpchTable.getTables());
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
        Thread.sleep(10);
        Logger log = Logger.get(IcebergQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

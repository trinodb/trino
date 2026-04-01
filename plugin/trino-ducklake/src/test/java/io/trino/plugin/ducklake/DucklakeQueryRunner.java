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
package io.trino.plugin.ducklake;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class DucklakeQueryRunner
{
    private static final String CATALOG = "ducklake";

    private DucklakeQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("test_schema")
                    .build());
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                // Generate test catalog with DuckDB if not already present
                Path catalogPath = Path.of("target/test-catalog/catalog.db");
                if (!Files.exists(catalogPath)) {
                    synchronized (DucklakeCatalogGenerator.class) {
                        if (!Files.exists(catalogPath)) {
                            DucklakeCatalogGenerator.generateTestCatalog();
                        }
                    }
                }

                // Build connector properties
                Map<String, String> properties = ImmutableMap.<String, String>builder()
                        .put("ducklake.catalog.database-url", "jdbc:sqlite:" + catalogPath.toAbsolutePath())
                        .put("ducklake.data-path", catalogPath.getParent().toAbsolutePath().toString())
                        .put("fs.hadoop.enabled", "true")
                        .putAll(connectorProperties.buildOrThrow())
                        .buildOrThrow();

                queryRunner.installPlugin(new DucklakePlugin());
                queryRunner.createCatalog(CATALOG, "ducklake", properties);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();

        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = DucklakeQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();

        Logger log = Logger.get(DucklakeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

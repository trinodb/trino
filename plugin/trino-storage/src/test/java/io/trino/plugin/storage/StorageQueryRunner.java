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
package io.trino.plugin.storage;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Minio;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public final class StorageQueryRunner
{
    private StorageQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("storage")
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new StoragePlugin());
                queryRunner.createCatalog("storage", "storage", connectorProperties);
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        //noinspection resource
        Minio minio = Minio.builder().build();
        minio.start();

        //noinspection resource
        DistributedQueryRunner queryRunner = StorageQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addConnectorProperty("fs.hadoop.enabled", "false")
                .addConnectorProperty("fs.native-s3.enabled", "true")
                .addConnectorProperty("s3.endpoint", minio.getMinioAddress())
                .addConnectorProperty("s3.aws-access-key", MINIO_ACCESS_KEY)
                .addConnectorProperty("s3.aws-secret-key", MINIO_SECRET_KEY)
                .addConnectorProperty("s3.region", MINIO_REGION)
                .addConnectorProperty("s3.path-style-access", "true")
                .build();

        Logger log = Logger.get(StorageQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

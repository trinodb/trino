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
package io.trino.plugin.hive.gcs;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public final class GcsHiveQueryRunner
{
    static {
        Logging.initialize();
    }

    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));

    private GcsHiveQueryRunner() {}

    public static HiveHadoop createGcsHiveContainer(String gcpCredentialKey)
            throws Exception
    {
        String jsonKeyBytes = new String(Base64.getDecoder().decode(gcpCredentialKey), UTF_8);
        Path gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json", READ_ONLY_PERMISSIONS);
        gcpCredentialsFile.toFile().deleteOnExit();
        Files.writeString(gcpCredentialsFile, jsonKeyBytes);

        return HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withNetwork(newNetwork())
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", getPathFromClassPathResource("io/trino/plugin/hive/gcs/hdp3.1-core-site.xml"),
                        "/etc/hadoop/conf/gcp-credentials.json", gcpCredentialsFile.toAbsolutePath().toString()))
                .build();
    }

    public static Builder builder(URI hiveMetastoreEndpoint)
    {
        return new Builder()
                .setHiveMetastoreEndpoint(hiveMetastoreEndpoint);
    }

    public static class Builder
            extends HiveQueryRunner.Builder<Builder>
    {
        private URI hiveMetastoreEndpoint;
        private String gcpStorageBucket;
        private String gcpCredentialKey;

        @CanIgnoreReturnValue
        public Builder setHiveMetastoreEndpoint(URI endpoint)
        {
            this.hiveMetastoreEndpoint = requireNonNull(endpoint, "hiveMetastoreEndpoint is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setGcpStorageBucket(String bucket)
        {
            this.gcpStorageBucket = requireNonNull(bucket, "gcpStorageBucket is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setGcpCredentialKey(String credentialKey)
        {
            this.gcpCredentialKey = requireNonNull(credentialKey, "gcpCredentialKey is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            requireNonNull(hiveMetastoreEndpoint, "hiveMetastoreEndpoint is null");
            requireNonNull(gcpStorageBucket, "gcpStorageBucket is null");
            requireNonNull(gcpCredentialKey, "gcpCredentialKey is null");

            byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
            String gcpCredentials = new String(jsonKeyBytes, UTF_8);

            addHiveProperty("fs.native-gcs.enabled", "true");
            addHiveProperty("gcs.json-key", gcpCredentials);
            addHiveProperty("hive.non-managed-table-writes-enabled", "true");
            addHiveProperty("hive.non-managed-table-creates-enabled", "true");

            setMetastore(distributedQueryRunner -> new BridgingHiveMetastore(
                    testingThriftHiveMetastoreBuilder()
                            .metastoreClient(hiveMetastoreEndpoint)
                            .build(distributedQueryRunner::registerResource)));

            setInitialSchemasLocationBase("gs://" + gcpStorageBucket);
            return super.build();
        }
    }

    static void main()
            throws Exception
    {
        String gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
        String gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");

        @SuppressWarnings("resource")
        HiveHadoop hiveHadoop = createGcsHiveContainer(gcpCredentialKey);
        hiveHadoop.start();

        @SuppressWarnings("resource")
        QueryRunner queryRunner = builder(hiveHadoop.getHiveMetastoreEndpoint())
                .setGcpStorageBucket(gcpStorageBucket)
                .setGcpCredentialKey(gcpCredentialKey)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                .setSkipTimezoneSetup(true)
                .setCreateTpchSchemas(false)
                .build();

        Logger log = Logger.get(GcsHiveQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

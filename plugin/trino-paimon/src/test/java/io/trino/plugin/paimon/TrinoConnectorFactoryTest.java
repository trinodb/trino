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
package io.trino.plugin.paimon;

import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorContext;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TrinoConnectorFactoryTest
{
    @TempDir
    Path tempFile;

    @Test
    public void testCreateConnector()
    {
        Map<String, String> config = Map.of("warehouse", tempFile.toString());
        ConnectorFactory factory = new PaimonConnectorFactory();
        Connector connector = factory.create("paimon", config, new TestingConnectorContext());
        assertThat(connector).isNotNull();
    }

    @Test
    public void testCreateConnectorAcceptsPaimonCatalogOptions()
    {
        Map<String, String> config = Map.ofEntries(
                Map.entry("warehouse", tempFile.toString()),
                Map.entry("metastore", "jdbc"),
                Map.entry("uri", "jdbc:postgresql://localhost:5432/paimon"),
                Map.entry("catalog-key", "prod"),
                Map.entry("lock-key-max-length", "128"),
                Map.entry("case-sensitive", "true"),
                Map.entry("sync-all-properties", "false"),
                Map.entry("cache-enabled", "false"),
                Map.entry("cache.expiration-interval", "10 min"),
                Map.entry("cache.manifest.max-memory", "256 MB"),
                Map.entry("local-cache.enabled", "true"),
                Map.entry("local-cache.dir", tempFile.resolve("cache").toString()),
                Map.entry("allow-upper-case", "true"),
                Map.entry("hive-conf-dir", tempFile.toString()),
                Map.entry("hadoop-conf-dir", tempFile.toString()),
                Map.entry("metastore.client.class", "org.apache.hadoop.hive.metastore.HiveMetaStoreClient"),
                Map.entry("location-in-properties", "true"),
                Map.entry("client-pool-cache.eviction-interval-ms", "60000"),
                Map.entry("hive.skip-update-stats", "true"),
                Map.entry("client-pool-cache.keys", "user_name"),
                Map.entry("alter-table-cascade", "false"),
                Map.entry("s3.access.key", "access"),
                Map.entry("s3.secret.key", "secret"),
                Map.entry("s3.path.style.access", "true"),
                Map.entry("s3a.endpoint", "http://localhost:9000"),
                Map.entry("s3a.endpoint.region", "us-east-1"),
                Map.entry("fs.s3a.signing-algorithm", "custom-signer"));
        ConnectorFactory factory = new PaimonConnectorFactory();

        Connector connector = factory.create("paimon", config, new TestingConnectorContext());

        assertThat(connector).isNotNull();
    }

    @Test
    public void testConnectorShutdownDoesNotPropagateCatalogCloseFailure()
    {
        AtomicBoolean closeCalled = new AtomicBoolean();
        PaimonConnector connector = new PaimonConnector(
                new ConnectorMetadata() {},
                new ConnectorSplitManager()
                {
                    @Override
                    public ConnectorSplitSource getSplits(
                            ConnectorTransactionHandle transaction,
                            ConnectorSession session,
                            ConnectorTableHandle table,
                            Set<ColumnHandle> dynamicFilterColumns,
                            Constraint constraint)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new ConnectorPageSourceProvider()
                {
                    @Override
                    public ConnectorPageSource createPageSource(
                            ConnectorTransactionHandle transaction,
                            ConnectorSession session,
                            ConnectorSplit split,
                            ConnectorTableHandle table,
                            Optional<ConnectorTableCredentials> tableCredentials,
                            List<ColumnHandle> columns,
                            DynamicFilter dynamicFilter)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new ConnectorPageSinkProvider()
                {
                    @Override
                    public ConnectorPageSink createPageSink(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorOutputTableHandle outputTableHandle,
                            Optional<ConnectorTableCredentials> tableCredentials,
                            ConnectorPageSinkId pageSinkId)
                    {
                        throw new UnsupportedOperationException("not used");
                    }

                    @Override
                    public ConnectorPageSink createPageSink(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorInsertTableHandle insertTableHandle,
                            Optional<ConnectorTableCredentials> tableCredentials,
                            ConnectorPageSinkId pageSinkId)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new ConnectorNodePartitioningProvider()
                {
                    @Override
                    public BucketFunction getBucketFunction(
                            ConnectorTransactionHandle transactionHandle,
                            ConnectorSession session,
                            ConnectorPartitioningHandle partitioningHandle,
                            List<Type> partitionChannelTypes,
                            int bucketCount)
                    {
                        throw new UnsupportedOperationException("not used");
                    }
                },
                new FailingClosePaimonCatalog(closeCalled),
                new PaimonSchemaProperties(),
                new PaimonTableOptions(),
                new PaimonSessionProperties(),
                Set.of(),
                new FunctionProvider() {});

        assertThatCode(connector::shutdown).doesNotThrowAnyException();
        assertThat(closeCalled).isTrue();
    }

    @Test
    public void testMetadataFactoryRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonMetadataFactory(null, _ -> {
            throw new UnsupportedOperationException("not used");
        }, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> new PaimonMetadataFactory(new Options(), null, TESTING_TYPE_MANAGER))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fileSystemFactory is null");
        assertThatThrownBy(() -> new PaimonMetadataFactory(new Options(), _ -> {
            throw new UnsupportedOperationException("not used");
        }, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("typeManager is null");
    }

    @Test
    public void testHadoopXmlConfigurationIsLoadedWithPaimonSemantics()
            throws Exception
    {
        Path firstXml = tempFile.resolve("core-site.xml");
        Files.writeString(firstXml,
                """
                <configuration>
                    <property>
                        <name>fs.defaultFS</name>
                        <value>s3://first</value>
                    </property>
                    <property>
                        <name>blank.value</name>
                        <value>   </value>
                    </property>
                    <property>
                        <name>missing.value</name>
                    </property>
                    <property>
                        <value>missing.name</value>
                    </property>
                    <property>
                        <name>   </name>
                        <value>blank.name</value>
                    </property>
                    <property>
                        <name>
                            explicit.key
                        </name>
                        <value>xml-value</value>
                    </property>
                </configuration>
                """);

        Path secondXml = tempFile.resolve("hdfs-site.xml");
        Files.writeString(secondXml,
                """
                <configuration>
                    <property>
                        <name>fs.defaultFS</name>
                        <value>s3://second</value>
                    </property>
                    <property>
                        <name>new.key</name>
                        <value>new-value</value>
                    </property>
                </configuration>
                """);

        Map<String, String> config = new HashMap<>();
        config.put("hadoop.explicit.key", "catalog-value");
        Set<String> protectedConfigKeys = Set.copyOf(config.keySet());

        PaimonConnectorFactory.readHadoopXml(firstXml.toString(), config, protectedConfigKeys);
        PaimonConnectorFactory.readHadoopXml(secondXml.toString(), config, protectedConfigKeys);

        assertThat(config)
                .containsEntry("hadoop.fs.defaultFS", "s3://second")
                .containsEntry("hadoop.new.key", "new-value")
                .containsEntry("hadoop.explicit.key", "catalog-value")
                .doesNotContainKeys("hadoop.blank.value", "hadoop.missing.value", "hadoop.missing.name", "hadoop.");
    }

    @Test
    public void testHadoopXmlConfigurationRejectsDoctypeAndExternalEntities()
            throws Exception
    {
        Path secret = tempFile.resolve("secret.txt");
        Files.writeString(secret, "secret-value");
        Path maliciousXml = tempFile.resolve("malicious-site.xml");
        Files.writeString(maliciousXml,
                """
                <!DOCTYPE configuration [
                    <!ENTITY xxe SYSTEM "%s">
                ]>
                <configuration>
                    <property>
                        <name>fs.defaultFS</name>
                        <value>&xxe;</value>
                    </property>
                </configuration>
                """.formatted(secret.toUri()));

        Map<String, String> config = new HashMap<>();

        assertThatThrownBy(() -> PaimonConnectorFactory.readHadoopXml(maliciousXml.toString(), config, Set.of()))
                .hasMessageContaining("DOCTYPE");
        assertThat(config).doesNotContainKey("hadoop.fs.defaultFS");
    }

    @Test
    public void testPaimonObjectStoreCredentialsAreMappedToTrinoNativeCredentials()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testPaimonFallbackObjectStoreCredentialsAreMappedToTrinoNativeCredentials()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access.key", "paimon-access");
        config.put("s3.secret.key", "paimon-secret");
        config.put("s3.path.style.access", "true");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "paimon-access")
                .containsEntry("s3.secret-key", "paimon-secret")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testPaimonS3AObjectStorePropertiesAreMappedToTrinoNativeProperties()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3a.access.key", "paimon-access");
        config.put("s3a.secret.key", "paimon-secret");
        config.put("s3a.endpoint", "http://localhost:9000");
        config.put("s3a.path.style.access", "true");
        config.put("s3a.endpoint.region", "us-east-1");
        config.put("s3a.signing-algorithm", "custom-signer");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "paimon-access")
                .containsEntry("s3.secret-key", "paimon-secret")
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.region", "us-east-1")
                .containsEntry("s3.signer-type", "custom-signer")
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testHadoopS3AObjectStorePropertiesAreMappedToTrinoNativeProperties()
    {
        Map<String, String> config = new HashMap<>();
        config.put("fs.s3a.access.key", "paimon-access");
        config.put("fs.s3a.secret.key", "paimon-secret");
        config.put("fs.s3a.endpoint", "http://localhost:9000");
        config.put("fs.s3a.path.style.access", "true");
        config.put("fs.s3a.endpoint.region", "us-east-1");
        config.put("fs.s3a.signing-algorithm", "custom-signer");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "paimon-access")
                .containsEntry("s3.secret-key", "paimon-secret")
                .containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.path-style-access", "true")
                .containsEntry("s3.region", "us-east-1")
                .containsEntry("s3.signer-type", "custom-signer")
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testTrinoNativeObjectStoreCredentialsAreMappedToPaimonCredentials()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "trino-access")
                .containsEntry("s3.secret-key", "trino-secret");
    }

    @Test
    public void testExplicitTrinoObjectStoreCredentialsArePreserved()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "trino-access")
                .containsEntry("s3.aws-secret-key", "trino-secret");
    }

    @Test
    public void testExplicitPaimonObjectStoreCredentialsArePreserved()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "paimon-access")
                .containsEntry("s3.secret-key", "paimon-secret");
    }

    @Test
    public void testBlankTrinoNativeObjectStoreCredentialsAreReplaced()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", "paimon-access");
        config.put("s3.secret-key", "paimon-secret");
        config.put("s3.aws-access-key", " ");
        config.put("s3.aws-secret-key", "\t");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.aws-access-key", "paimon-access")
                .containsEntry("s3.aws-secret-key", "paimon-secret");
    }

    @Test
    public void testBlankPaimonObjectStoreCredentialsAreReplaced()
    {
        Map<String, String> config = new HashMap<>();
        config.put("s3.access-key", " ");
        config.put("s3.secret-key", "\t");
        config.put("s3.aws-access-key", "trino-access");
        config.put("s3.aws-secret-key", "trino-secret");

        PaimonConnectorFactory.addS3CredentialProperties(config);

        assertThat(config)
                .containsEntry("s3.access-key", "trino-access")
                .containsEntry("s3.secret-key", "trino-secret");
    }

    private static class FailingClosePaimonCatalog
            extends PaimonCatalog
    {
        private final AtomicBoolean closeCalled;

        private FailingClosePaimonCatalog(AtomicBoolean closeCalled)
        {
            super(new Options(), _ -> {
                throw new UnsupportedOperationException("not used");
            });
            this.closeCalled = closeCalled;
        }

        @Override
        public void close()
                throws Exception
        {
            closeCalled.set(true);
            throw new IOException("close failed");
        }
    }
}

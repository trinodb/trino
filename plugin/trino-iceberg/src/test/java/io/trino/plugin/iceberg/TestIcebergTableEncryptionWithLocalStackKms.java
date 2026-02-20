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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.encryption.IcebergEncryptionManagerFactory;
import io.trino.plugin.iceberg.encryption.TestingLocalStackAwsClientFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.stream.StreamSupport;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableEncryptionWithLocalStackKms
        extends AbstractTestQueryFramework
{
    private static final DockerImageName LOCALSTACK_IMAGE = DockerImageName.parse("localstack/localstack:4.0.3");
    private static final int LOCALSTACK_PORT = 4566;
    private static final String AWS_KMS_IMPL = "org.apache.iceberg.aws.AwsKeyManagementClient";

    private GenericContainer<?> localstack;
    private String kmsKeyArn;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        localstack = closeAfterClass(new GenericContainer<>(LOCALSTACK_IMAGE)
                .withEnv("SERVICES", "kms")
                .withEnv("EAGER_SERVICE_LOADING", "1")
                .withEnv("AWS_DEFAULT_REGION", Region.US_EAST_1.id())
                .withExposedPorts(LOCALSTACK_PORT)
                .waitingFor(Wait.forHttp("/_localstack/health")
                        .forPort(LOCALSTACK_PORT)
                        .forStatusCode(200))
                .withStartupTimeout(Duration.ofMinutes(2)));
        localstack.start();

        URI kmsEndpoint = localstackEndpoint();
        kmsKeyArn = createKmsKey(kmsEndpoint);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.encryption.kms-impl", AWS_KMS_IMPL)
                .addIcebergProperty("iceberg.allowed-extra-properties", "client.factory," + TestingLocalStackAwsClientFactory.KMS_ENDPOINT_PROPERTY)
                .build();
    }

    @Test
    public void testEncryptedTableOperationsWithLocalStackKms()
    {
        String localstackKmsEndpoint = localstackEndpoint().toString();
        String tableDefinition = format(
                "(id INT, data VARCHAR) WITH (" +
                        "format_version = 3, " +
                        "encryption_key_id = '%s', " +
                        "encryption_data_key_length = 16, " +
                        "extra_properties = MAP(ARRAY['client.factory', '%s'], ARRAY['%s', '%s']))",
                kmsKeyArn,
                TestingLocalStackAwsClientFactory.KMS_ENDPOINT_PROPERTY,
                TestingLocalStackAwsClientFactory.class.getName(),
                localstackKmsEndpoint);

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_encrypted_table_localstack_",
                tableDefinition)) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
            assertUpdate("UPDATE " + tableName + " SET data = 'bb' WHERE id = 2", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertQuery("SELECT id, data FROM " + tableName + " ORDER BY id", "VALUES (2, 'bb'), (3, 'c')");
            assertEncryptedFilesHaveKeyMetadata(tableName);
        }
    }

    private URI localstackEndpoint()
    {
        return URI.create("http://" + localstack.getHost() + ":" + localstack.getMappedPort(LOCALSTACK_PORT));
    }

    private static String createKmsKey(URI endpoint)
    {
        try (KmsClient kmsClient = KmsClient.builder()
                .endpointOverride(endpoint)
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {
            return kmsClient.createKey(request -> request.description("Trino Iceberg encryption test key"))
                    .keyMetadata()
                    .arn();
        }
    }

    private void assertEncryptedFilesHaveKeyMetadata(String tableName)
    {
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        IcebergConfig icebergConfig = new IcebergConfig().setEncryptionKmsImpl(AWS_KMS_IMPL);
        IcebergEncryptionManagerFactory encryptionManagerFactory = new IcebergEncryptionManagerFactory(icebergConfig);
        BaseTable icebergTable = loadTable(
                tableName,
                metastore,
                fileSystemFactory,
                ICEBERG_CATALOG,
                getSession().getSchema().orElseThrow(),
                encryptionManagerFactory);

        List<FileScanTask> scanTasks = StreamSupport.stream(icebergTable.newScan().planFiles().spliterator(), false)
                .toList();
        assertThat(scanTasks).isNotEmpty();
        scanTasks.forEach(task -> assertThat(task.file().keyMetadata()).isNotNull());

        List<DeleteFile> deleteFiles = scanTasks.stream()
                .flatMap(task -> task.deletes().stream())
                .toList();
        deleteFiles.forEach(deleteFile -> assertThat(deleteFile.keyMetadata()).isNotNull());
    }
}

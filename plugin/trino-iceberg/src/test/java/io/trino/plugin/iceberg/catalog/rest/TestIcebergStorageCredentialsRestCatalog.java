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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergStorageCredentialsRestCatalog
        extends AbstractTestQueryFramework
{
    private Path warehouseLocation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backendCatalog(warehouseLocation))
                .addAllCredentials(List.of(
                        ImmutableCredential.builder()
                                .prefix("file://")
                                .config(ImmutableMap.of(
                                        "s3.access-key-id", "test-vended-access-key",
                                        "s3.secret-access-key", "test-vended-secret-key",
                                        "s3.session-token", "test-vended-session-token"))
                                .build()))
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .buildOrThrow())
                .build();
    }

    @Test
    public void testCreateAndReadTableWithStorageCredentials()
    {
        assertUpdate("CREATE SCHEMA test_storage_creds");
        assertUpdate("CREATE TABLE test_storage_creds.test_table (id INTEGER, name VARCHAR)");
        assertUpdate("INSERT INTO test_storage_creds.test_table VALUES (1, 'alice'), (2, 'bob')", 2);

        assertThat(query("SELECT * FROM test_storage_creds.test_table"))
                .matches("VALUES (1, VARCHAR 'alice'), (2, VARCHAR 'bob')");

        assertUpdate("DROP TABLE test_storage_creds.test_table");
        assertUpdate("DROP SCHEMA test_storage_creds");
    }

    @Test
    public void testStorageCredentialsWithMultipleOperations()
    {
        assertUpdate("CREATE SCHEMA test_multi_ops");
        assertUpdate("CREATE TABLE test_multi_ops.t1 (x INTEGER)");
        assertUpdate("INSERT INTO test_multi_ops.t1 VALUES 1, 2, 3", 3);

        // Read back
        assertThat(query("SELECT count(*) FROM test_multi_ops.t1"))
                .matches("VALUES BIGINT '3'");

        // Additional insert (triggers new table load with credentials)
        assertUpdate("INSERT INTO test_multi_ops.t1 VALUES 4, 5", 2);
        assertThat(query("SELECT count(*) FROM test_multi_ops.t1"))
                .matches("VALUES BIGINT '5'");

        assertUpdate("DROP TABLE test_multi_ops.t1");
        assertUpdate("DROP SCHEMA test_multi_ops");
    }
}

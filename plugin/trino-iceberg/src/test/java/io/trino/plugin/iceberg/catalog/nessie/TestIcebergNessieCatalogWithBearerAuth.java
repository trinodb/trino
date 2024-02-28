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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.containers.KeycloakContainer;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

public class TestIcebergNessieCatalogWithBearerAuth
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Network network = closeAfterClass(Network.newNetwork());

        KeycloakContainer keycloakContainer = closeAfterClass(KeycloakContainer.builder().withNetwork(network).build());
        keycloakContainer.start();

        Map<String, String> envVars = ImmutableMap.<String, String>builder()
                .putAll(NessieContainer.DEFAULT_ENV_VARS)
                .put("QUARKUS_OIDC_AUTH_SERVER_URL", KeycloakContainer.SERVER_URL + "/realms/master")
                .put("QUARKUS_OIDC_CLIENT_ID", "projectnessie")
                .put("NESSIE_SERVER_AUTHENTICATION_ENABLED", "true")
                .buildOrThrow();

        NessieContainer nessieContainer = closeAfterClass(NessieContainer.builder().withEnvVars(envVars).withNetwork(network).build());
        nessieContainer.start();

        Path tempDir = Files.createTempDirectory("test_trino_nessie_catalog");
        closeAfterClass(() -> deleteRecursively(tempDir, ALLOW_INSECURE));

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "nessie")
                .put("iceberg.nessie-catalog.uri", nessieContainer.getRestApiUri())
                .put("iceberg.nessie-catalog.default-warehouse-dir", tempDir.toString())
                .put("iceberg.nessie-catalog.authentication.type", "BEARER")
                .put("iceberg.nessie-catalog.authentication.token", keycloakContainer.getAccessToken())
                .buildOrThrow();

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(properties)
                .build();
    }

    @Test
    public void testWithValidAccessToken()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_valid_access_token", "(a INT, b VARCHAR)", ImmutableList.of("(1, 'a')"))) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES(1, 'a')");
        }
    }
}

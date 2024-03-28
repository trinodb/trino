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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.Failure;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@TestInstance(PER_CLASS)
public class TestIcebergGlueTableOperationsInsertFailure
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGlueTableOperationsInsertFailure.class);

    private static final String ICEBERG_CATALOG = "iceberg";

    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();

    private HiveMetastore glueHiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(schemaName)
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        AWSGlueAsyncAdapterProvider awsGlueAsyncAdapterProvider = delegate -> newProxy(AWSGlueAsync.class, (proxy, method, methodArgs) -> {
            Object result;
            try {
                result = method.invoke(delegate, methodArgs);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
            if (method.getName().equals("updateTable")) {
                throw new RuntimeException("Test-simulated Glue timeout exception");
            }
            return result;
        });

        Path dataDirectory = Files.createTempDirectory("iceberg_data");
        dataDirectory.toFile().deleteOnExit();

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory, Optional.of(new TestingIcebergGlueCatalogModule(awsGlueAsyncAdapterProvider))));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of());

        glueHiveMetastore = createTestingGlueHiveMetastore(dataDirectory);

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .setLocation(Optional.of(dataDirectory.toString()))
                .build();
        glueHiveMetastore.createDatabase(database);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueHiveMetastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                glueHiveMetastore.dropDatabase(schemaName, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", schemaName);
        }
    }

    @Test
    public void testInsertFailureDoesNotCorruptTheTableMetadata()
    {
        String tableName = "test_insert_failure" + randomNameSuffix();

        getQueryRunner().execute(format("CREATE TABLE %s (a_varchar) AS VALUES ('Trino')", tableName));
        assertThatThrownBy(() -> getQueryRunner().execute("INSERT INTO " + tableName + " VALUES 'rocks'"))
                .satisfies(throwable -> {
                    assertThat(throwable).isInstanceOf(QueryFailedException.class);
                    assertThat(throwable.getCause()).isInstanceOf(Failure.class);
                    Failure failure = (Failure) throwable.getCause();
                    assertThat(failure.getMessage()).contains("Test-simulated Glue timeout exception");
                    assertThat(failure.getFailureInfo().getType()).isEqualTo("org.apache.iceberg.exceptions.CommitStateUnknownException");
                });
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }
}

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
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.reflect.Reflection.newProxy;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestIcebergGlueTableOperationsInsertFailure
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGlueTableOperationsInsertFailure.class);

    private static final String ICEBERG_CATALOG = "iceberg";

    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();

    private GlueHiveMetastore glueHiveMetastore;

    @Override
    protected LocalQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(schemaName)
                .build();
        LocalQueryRunner queryRunner = LocalQueryRunner.create(session);

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

        InternalFunctionBundle.InternalFunctionBundleBuilder functions = InternalFunctionBundle.builder();
        new IcebergPlugin().getFunctions().forEach(functions::functions);
        queryRunner.addFunctions(functions.build());

        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                new TestingIcebergConnectorFactory(Optional.of(new TestingIcebergGlueCatalogModule(awsGlueAsyncAdapterProvider)), Optional.empty(), EMPTY_MODULE),
                ImmutableMap.of());

        Path dataDirectory = Files.createTempDirectory("iceberg_data");
        dataDirectory.toFile().deleteOnExit();

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

    @AfterClass(alwaysRun = true)
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
                .isInstanceOf(CommitStateUnknownException.class)
                .hasMessageContaining("Test-simulated Glue timeout exception");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }
}

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
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.OperationTimeoutException;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.TestingIcebergConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.reflect.Reflection.newProxy;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
@Test(singleThreaded = true) // testException is a shared mutable state
public class TestIcebergGlueCreateTableFailure
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGlueCreateTableFailure.class);

    private static final String ICEBERG_CATALOG = "iceberg";

    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();

    private Path dataDirectory;
    private TrinoFileSystem fileSystem;
    private GlueHiveMetastore glueHiveMetastore;
    private final AtomicReference<RuntimeException> testException = new AtomicReference<>();

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
            if (method.getName().equals("createTable")) {
                throw testException.get();
            }
            try {
                result = method.invoke(delegate, methodArgs);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
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

        dataDirectory = Files.createTempDirectory("test_iceberg_create_table_failure");
        dataDirectory.toFile().deleteOnExit();

        glueHiveMetastore = createTestingGlueHiveMetastore(dataDirectory.toString());
        fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT).create(TestingConnectorSession.SESSION);

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
                glueHiveMetastore.dropDatabase(schemaName, false);
            }
            if (dataDirectory != null) {
                deleteRecursively(dataDirectory, ALLOW_INSECURE);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", schemaName);
        }
    }

    @Test
    public void testCreateTableFailureMetadataCleanedUp()
            throws Exception
    {
        final String exceptionMessage = "Test-simulated metastore invalid input exception";
        testException.set(new InvalidInputException(exceptionMessage));
        testCreateTableFailure(exceptionMessage, false);
    }

    @Test
    public void testCreateTableFailureMetadataNotCleanedUp()
            throws Exception
    {
        final String exceptionMessage = "Test-simulated metastore operation timeout exception";
        testException.set(new OperationTimeoutException(exceptionMessage));
        testCreateTableFailure(exceptionMessage, true);
    }

    private void testCreateTableFailure(String expectedExceptionMessage, boolean shouldMetadataFileExist)
            throws Exception
    {
        String tableName = "test_create_failure_" + randomNameSuffix();
        assertThatThrownBy(() -> getQueryRunner().execute("CREATE TABLE " + tableName + " (a_varchar) AS VALUES ('Trino')"))
                .hasMessageContaining(expectedExceptionMessage);

        assertMetadataLocation(tableName, shouldMetadataFileExist);
    }

    protected void assertMetadataLocation(String tableName, boolean shouldMetadataFileExist)
            throws Exception
    {
        FileIterator fileIterator = fileSystem.listFiles(dataDirectory.toString());
        String tableLocationPrefix = Path.of(dataDirectory.toString(), tableName).toString();
        boolean metadataFileFound = false;
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            String location = fileEntry.location();
            if (location.startsWith(tableLocationPrefix) && location.endsWith(".metadata.json")) {
                metadataFileFound = true;
                break;
            }
        }
        if (shouldMetadataFileExist) {
            assertThat(metadataFileFound).as("Metadata file should exist").isTrue();
        }
        else {
            assertThat(metadataFileFound).as("Metadata file should not exist").isFalse();
        }
    }
}

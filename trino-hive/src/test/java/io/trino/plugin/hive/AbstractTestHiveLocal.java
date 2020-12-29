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
package io.prestosql.plugin.hive;

import com.google.common.io.Files;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrincipalType;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTestHiveLocal
        extends AbstractTestHive
{
    private static final String DEFAULT_TEST_DB_NAME = "test";
    private static final HiveIdentity HIVE_IDENTITY = new HiveIdentity(SESSION);

    private File tempDir;
    private final String testDbName;

    protected AbstractTestHiveLocal()
    {
        this(DEFAULT_TEST_DB_NAME);
    }

    protected AbstractTestHiveLocal(String testDbName)
    {
        this.testDbName = requireNonNull(testDbName, "testDbName is null");
    }

    protected abstract HiveMetastore createMetastore(File tempDir);

    @BeforeClass(alwaysRun = true)
    public void initialize()
    {
        tempDir = Files.createTempDir();

        HiveMetastore metastore = createMetastore(tempDir);

        metastore.createDatabase(HIVE_IDENTITY,
                Database.builder()
                        .setDatabaseName(testDbName)
                        .setOwnerName("public")
                        .setOwnerType(PrincipalType.ROLE)
                        .build());

        HiveConfig hiveConfig = new HiveConfig()
                .setParquetTimeZone("America/Los_Angeles")
                .setRcfileTimeZone("America/Los_Angeles");

        setup(testDbName, hiveConfig, metastore, HDFS_ENVIRONMENT);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        try {
            getMetastoreClient().dropDatabase(HIVE_IDENTITY, testDbName);
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected ConnectorTableHandle getTableHandle(ConnectorMetadata metadata, SchemaTableName tableName)
    {
        if (tableName.getTableName().startsWith(TEMPORARY_TABLE_PREFIX)) {
            return super.getTableHandle(metadata, tableName);
        }
        throw new SkipException("tests using existing tables are not supported");
    }

    @Override
    public void testGetAllTableNames()
    {
        throw new SkipException("Test disabled for this subclass");
    }

    @Override
    public void testGetAllTableColumns()
    {
        throw new SkipException("Test disabled for this subclass");
    }

    @Override
    public void testGetAllTableColumnsInSchema()
    {
        throw new SkipException("Test disabled for this subclass");
    }

    @Override
    public void testGetTableNames()
    {
        throw new SkipException("Test disabled for this subclass");
    }

    @Override
    public void testGetTableSchemaOffline()
    {
        throw new SkipException("Test disabled for this subclass");
    }
}

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
package io.trino.plugin.hive.metastore.dlf;

import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.dlf.exceptions.InvalidOperationException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAlibabaDlfDatabase
{
    private static HiveMetastore client;
    private static String testDB = TestingAlibabaDlfUtils.TEST_DB;

    @BeforeClass
    public static void setUp() throws IOException
    {
        client = TestingAlibabaDlfUtils.getDlfClient();
    }

    @AfterClass
    public void cleanUp()
    {
        try {
            client.dropDatabase(testDB, true);
        }
        catch (SchemaNotFoundException e) {
            System.out.println("database not found");
        }
    }

    @Test
    public void testCreateDatabase()
    {
        String databaseName = testDB;
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(databaseName).toString();
        Database database = TestingAlibabaDlfUtils.getDatabase(databaseName, location);

        client.createDatabase(database);

        Optional<Database> check = client.getDatabase(databaseName);
        assertTrue(check.isPresent());
        assertEquals(databaseName, check.get().getDatabaseName());
        assertTrue(check.get().getLocation().isPresent());
        assertEquals(location, check.get().getLocation().get());
        client.dropDatabase(testDB, true);
    }

    @Test
    public void testCreateDatabaseRepeatedly()
    {
        String databaseName = testDB;
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(databaseName).toString();
        Database database = TestingAlibabaDlfUtils.getDatabase(databaseName, location);

        client.createDatabase(database);

        assertThatThrownBy(() -> client.createDatabase(database))
                .isInstanceOf(SchemaAlreadyExistsException.class);
        client.dropDatabase(testDB, true);
    }

    @Test
    public void testAlterDatabase() throws Exception
    {
        String databaseName = testDB;
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(databaseName).toString();
        Database database = TestingAlibabaDlfUtils.getDatabase(databaseName, location);
        client.createDatabase(database);
        String newDatabaseName = "new" + testDB;
        try {
            client.renameDatabase(databaseName, newDatabaseName);
        }
        catch (InvalidOperationException e) {
        }
        client.dropDatabase(databaseName, true);
    }

    @Test
    public void testDropDatabase() throws Exception
    {
        String databaseName = testDB;
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(databaseName).toString();
        Database database = TestingAlibabaDlfUtils.getDatabase(databaseName, location);
        // deleteData
        client.createDatabase(database);
        client.dropDatabase(databaseName, true);

        assertFalse(Files.exists(Paths.get(location.substring(5))), "Database data should be deleted");
    }

    @Test
    public void testGetDatabase() throws Exception
    {
        // GetDatabase
        assertEquals("default", client.getDatabase("default").get().getDatabaseName());
        try {
            client.getDatabase("db_not_exists");
        }
        catch (TrinoException e) {
            assertEquals(NOT_FOUND.toErrorCode(), e.getErrorCode());
        }

        // GetAllDatabases
        assertTrue(client.getAllDatabases().size() >= 1);
    }
}

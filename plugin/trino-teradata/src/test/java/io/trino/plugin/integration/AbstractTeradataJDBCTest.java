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

package io.trino.plugin.integration;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Abstract base class for Teradata JDBC integration tests in Trino.
 * <p>
 * This class initializes a Teradata test database using a provided {@code databaseName},
 * manages its lifecycle, and provides support for creating and inserting data
 * via {@link DataSetup}.
 * </p>
 *
 * <p>Derived classes must implement {@link #initTables()} to set up the schema
 * and tables needed for testing.</p>
 *
 * <p>Database is created before all tests and dropped after all tests. A unique
 * {@code databaseName} can be passed by subclasses to allow isolated parallel test runs.</p>
 */
abstract class AbstractTeradataJDBCTest
        extends AbstractTestQueryFramework
{
    /**
     * Teradata database wrapper for executing SQL and retrieving connections.
     */
    protected TestingTeradataServer database;
    protected String envName;

    /**
     * Constructs the test framework with a specific database name.
     */
    AbstractTeradataJDBCTest(String envName)
    {
        this.envName = envName;
    }

    /**
     * Creates a {@link QueryRunner} instance for Trino.
     *
     * @return a new QueryRunner for executing Trino queries
     * @throws Exception if initialization fails
     */
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database = new TestingTeradataServer(this.envName);
        // Register this specific instance for this test class
        return TeradataQueryRunner.builder(database).build();
    }

    /**
     * Provides a {@link DataSetup} that creates and inserts test data into a table
     * in the configured Teradata schema.
     *
     * @param tableNamePrefix prefix used for naming the test table
     * @return a DataSetup object to create and populate the table
     */
    protected DataSetup teradataJDBCCreateAndInsert(String tableNamePrefix)
    {
        String prefix = String.format("%s.%s", database.getDatabaseName(), tableNamePrefix);
        return new CreateAndInsertDataSetup(database, prefix);
    }

    /**
     * Sets up the test database before all tests run.
     * Creates the schema if it does not exist and invokes {@link #initTables()}.
     */
    @BeforeAll
    public void setup()
    {
        initTables();
    }

    /**
     * Cleans up the test database after all tests have run.
     * Deletes and drops the schema used for testing.
     */

    @AfterAll
    public void cleanupTestClass()
    {
        if (database != null) {
            database.close();
        }
    }

    /**
     * Implemented by subclasses to define the schema and tables required for the test.
     */
    protected abstract void initTables();
}

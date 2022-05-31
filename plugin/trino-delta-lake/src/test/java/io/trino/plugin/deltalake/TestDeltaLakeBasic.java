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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.testng.Assert.assertFalse;

public class TestDeltaLakeBasic
        extends AbstractTestQueryFramework
{
    private static final List<String> PERSON_TABLES = ImmutableList.of(
            "person", "person_without_last_checkpoint", "person_without_old_jsons", "person_without_checkpoints");
    private static final List<String> OTHER_TABLES = ImmutableList.of("no_column_stats");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner();
    }

    @BeforeClass
    public void registerTables()
    {
        for (String table : Iterables.concat(PERSON_TABLES, OTHER_TABLES)) {
            String dataPath = getTableLocation(table).toExternalForm();
            getQueryRunner().execute(
                    format("CREATE TABLE %s (name VARCHAR(256), age INTEGER) WITH (location = '%s')", table, dataPath));
        }
    }

    private URL getTableLocation(String table)
    {
        return getClass().getClassLoader().getResource("databricks/" + table);
    }

    @DataProvider
    public Object[][] tableNames()
    {
        return PERSON_TABLES.stream()
                .map(table -> new Object[] {table})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "tableNames")
    public void testDescribeTable(String tableName)
    {
        // the schema is actually defined in the transaction log
        assertQuery(
                format("DESCRIBE %s", tableName),
                "VALUES " +
                        "('name', 'varchar', '', ''), " +
                        "('age', 'integer', '', ''), " +
                        "('married', 'boolean', '', ''), " +
                        "('gender', 'varchar', '', ''), " +
                        "('phones', 'array(row(number varchar, label varchar))', '', ''), " +
                        "('address', 'row(street varchar, city varchar, state varchar, zip varchar)', '', ''), " +
                        "('income', 'double', '', '')");
    }

    @Test(dataProvider = "tableNames")
    public void testSimpleQueries(String tableName)
    {
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "VALUES 12");
        assertQuery(format("SELECT income FROM %s WHERE name = 'Bob'", tableName), "VALUES 99000.00");
        assertQuery(format("SELECT name FROM %s WHERE name LIKE 'B%%'", tableName), "VALUES ('Bob'), ('Betty')");
        assertQuery(format("SELECT DISTINCT gender FROM %s", tableName), "VALUES ('M'), ('F'), (null)");
        assertQuery(format("SELECT DISTINCT age FROM %s", tableName), "VALUES (21), (25), (28), (29), (30), (42)");
        assertQuery(format("SELECT name FROM %s WHERE age = 42", tableName), "VALUES ('Alice'), ('Emma')");
    }

    @Test
    public void testNoColumnStats()
    {
        // Data generated using:
        // CREATE TABLE no_column_stats
        // USING delta
        // LOCATION 's3://starburst-alex/delta/no_column_stats'
        // TBLPROPERTIES (delta.dataSkippingNumIndexedCols=0)   -- collects only table stats (row count), but no column stats
        // AS
        // SELECT 42 AS c_int, 'foo' AS c_str
        assertQuery("SELECT c_str FROM no_column_stats WHERE c_int = 42", "VALUES 'foo'");
    }

    @Test
    public void testDropTableBadLocation()
            throws IOException, URISyntaxException
    {
        // create a bad_person table which is based on person table in temporary location
        String tableName = "bad_person";
        Path tableLocation = Files.createTempFile("bad_person", null);
        copyDirectoryContents(Path.of(getTableLocation("person").toURI()), tableLocation);
        getQueryRunner().execute(
                format("CREATE TABLE %s (name VARCHAR(256), age INTEGER) WITH (location = '%s')", tableName, tableLocation));

        // break the table by deleting all its files including transaction log
        deleteRecursively(tableLocation, ALLOW_INSECURE);

        // try to drop table
        getQueryRunner().execute("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void copyDirectoryContents(Path source, Path destination)
            throws IOException
    {
        try (Stream<Path> stream = Files.walk(source)) {
            stream.forEach(file -> {
                try {
                    Files.copy(file, destination.resolve(source.relativize(file)), REPLACE_EXISTING);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}

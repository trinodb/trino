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
package io.prestosql.tests.ranger;

import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryExecutionException;
import io.prestodb.tempto.query.QueryExecutor;
import io.prestodb.tempto.query.QueryResult;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.SQLException;

import static com.google.common.io.Resources.getResource;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.RANGER;
import static io.prestosql.tests.utils.QueryExecutors.connectToPresto;

public class RangerTestCases
        extends ProductTest
{
    private QueryExecutor aliceExecutor;
    private QueryExecutor bobExecutor;
    private QueryExecutor charlieExecutor;
    private QueryExecutor adminExecutor;
    private static final String WORDCOUNT = "ranger/wordcount.txt";
    public static final String schema = "hive.rangerauthz";
    public static final String words = schema + ".words";
    public static final String words2 = schema + ".words2";
    final String selectStar = "SELECT * FROM " + words + " where count = 100";
    final String selectCount = "SELECT count FROM " + words + " where word = 'Mr.'";
    final String insertQuery = "insert into " + words + " ( word, count) values ('newword', 5)";
    final String newWordSelect = "SELECT * FROM " + words + " where word = 'newword'";
    private static final String useRangerSchema = "use " + schema;

    //@BeforeTestWithContext
    // @Requires(RangerTableRequirements.class)
    @Test(groups = {RANGER}, testName = "setup")
    public void setup()
            throws Exception
    {
        // Get a random port
        aliceExecutor = connectToPresto("alice@presto");
        bobExecutor = connectToPresto("bob@presto");
        charlieExecutor = connectToPresto("charlie@presto");
        adminExecutor = connectToPresto("admin@presto");

        // statement.execute("CREATE TABLE WORDS (word STRING, count INT)");

        adminExecutor.executeQuery("create schema if not exists " + schema);
        adminExecutor.executeQuery("drop table if exists " + words);
        adminExecutor.executeQuery("create table if not exists " + words + " ( word     VARCHAR, count          int )");

        // Copy "wordcount.txt" to "target" to avoid overwriting it during load
        StringBuilder stringBuilder = new StringBuilder();

        new BufferedReader(new InputStreamReader(getResource(WORDCOUNT).openStream())).lines().filter(str -> str.contains(" ")).forEach(str -> {
            System.out.println(str);
            String[] splitArr = str.split(" ", 2);
            stringBuilder.append("( '" + splitArr[0] + "' , " + splitArr[1] + " ) ,");
        });

        System.out.println(stringBuilder.toString());
        adminExecutor.executeQuery("insert into " + words + " values " + stringBuilder.substring(0, stringBuilder.length() - 1));

        // Just test to make sure it's working
        QueryResult queryResult = adminExecutor.executeQuery(this.selectStar);

        assertThat(queryResult).hasRowsCount(1);
        assertThat(queryResult).containsOnly(
                row("Mr.", 100));
    }

    // this should be allowed (by the policy - user)
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveSelectAllAsBob()
    {
        QueryResult queryResult = bobExecutor.executeQuery(selectStar);
        assertThat(queryResult).hasRowsCount(1);
        assertThat(queryResult).containsOnly(
                row("Mr.", 100));
    }

    // the "IT" group doesn't have permission to select all
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveSelectAllAsAlice()
    {
        try {
            aliceExecutor.executeQuery(selectStar);
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && ((SQLException) ex.getCause()).getCause().toString().equals(AccessDeniedException.class.getCanonicalName().toString() + ": Access Denied: Cannot access catalog hive"))) {
                throw ex;
            }
        }
    }

    // this should be allowed (by the policy - user)
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveSelectSpecificColumnAsBob()
            throws Exception
    {
        QueryResult queryResult = bobExecutor.executeQuery(selectCount);
        assertThat(queryResult).hasRowsCount(1);
        assertThat(queryResult).containsOnly(
                row(100));
    }

    // this should be allowed
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveSelectSpecificColumnAsAlice()
    {
        try {
            aliceExecutor.executeQuery(selectCount);
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && ((SQLException) ex.getCause()).getCause().toString().equals(AccessDeniedException.class.getCanonicalName().toString() + ": Access Denied: Cannot access catalog hive"))) {
                throw ex;
            }
        }
    }

    // this should be allowed (by the policy - user)
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveUpdateAllAsBob()
    {
        bobExecutor.executeQuery(insertQuery);

        QueryResult queryResult = bobExecutor.executeQuery(newWordSelect);
        assertThat(queryResult).hasRowsCount(1);
        assertThat(queryResult).containsOnly(
                row("newword", 5));
    }

    // this should not be allowed as "alice" can't insert into the table
    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveUpdateAllAsAlice()
    {
        try {
            aliceExecutor.executeQuery(insertQuery);
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && ((SQLException) ex.getCause()).getCause().toString().equals(AccessDeniedException.class.getCanonicalName().toString() + ": Access Denied: Cannot access catalog hive"))) {
                throw ex;
            }
        }
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testHiveCreateDropDatabase()
    {
        bobExecutor.executeQuery("CREATE schema if not exists hive.bobtemp");

        try {
            aliceExecutor.executeQuery("CREATE schema if not exists hive.alicetemp");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && ((SQLException) ex.getCause()).getCause().toString().equals(AccessDeniedException.class.getCanonicalName().toString() + ": Access Denied: Cannot access catalog hive"))) {
                throw ex;
            }
        }

        try {
            bobExecutor.executeQuery("drop schema hive.bobtemp");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && ((SQLException) ex.getCause()).getCause().toString().equals(AccessDeniedException.class.getCanonicalName().toString() + ": Access Denied: Cannot access catalog hive"))) {
                throw ex;
            }
        }
        adminExecutor.executeQuery("drop schema hive.bobtemp");
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testBobSelectOnDifferentDatabase()
    {
        adminExecutor.executeQuery("CREATE schema if not exists hive.admintemp");
        adminExecutor.executeQuery("CREATE TABLE if not exists  hive.admintemp.WORDS (word varchar, count INT)");

        try {
            bobExecutor.executeQuery("SELECT count FROM hive.admintemp.WORDS where count = 100");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }

        adminExecutor.executeQuery("drop TABLE hive.admintemp.words");
        adminExecutor.executeQuery("drop schema hive.admintemp");
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testBobAlter()
    {
        // Create a new table as admin
        adminExecutor.executeQuery("CREATE TABLE IF NOT EXISTS " + words2 + " (word varchar, count INT)");

        // Try to add a new column in words as "bob" - this should fail
        try {
            bobExecutor.executeQuery("ALTER TABLE " + words2 + " ADD COLUMN newcol varchar");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }

        // Now alter it as "admin"

        adminExecutor.executeQuery("ALTER TABLE " + words2 + " ADD COLUMN newcol varchar");

        // Try to alter it as "bob" - this should fail
        try {
            bobExecutor.executeQuery("ALTER TABLE " + words2 + " drop COLUMN newcol ");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }

        // Now alter it as "admin"
        adminExecutor.executeQuery("ALTER TABLE " + words2 + " drop COLUMN newcol ");

        // Clean up
        adminExecutor.executeQuery("drop TABLE " + words2);
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testBobSelectOnDifferentTables()
    {
        final String newTable = schema + ".words2";

        // Create a "words2" table in "rangerauthz"
        adminExecutor.executeQuery("CREATE TABLE if not exists " + newTable + "(word varchar, count INT)");

        // Now try to read it as "bob"
        try {
            bobExecutor.executeQuery("SELECT count FROM " + newTable + " where count = 100");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }
        adminExecutor.executeQuery("drop TABLE " + newTable);
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testGrantrevoke()
            throws Exception
    {
        adminExecutor.executeQuery("CREATE schema IF NOT EXISTS hive.rangerauthzx");
        adminExecutor.executeQuery("CREATE TABLE  if not exists hive.rangerauthzx.tbl1 (a INT, b INT)");

        try {
            charlieExecutor.executeQuery("grant select ON TABLE hive.rangerauthzx.tbl1 to USER admin with grant option");
            Assert.fail("access should not have been granted");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }
        adminExecutor.executeQuery("DROP TABLE hive.rangerauthzx.tbl1");
    }

    @Test(groups = {RANGER}, dependsOnMethods = "setup")
    public void testTagBasedPolicyForTable()
            throws Exception
    {
        // Create a database as "admin"
        adminExecutor.executeQuery("CREATE schema hive.hivetable");

        // Create a "words" table in "hivetable"
        adminExecutor.executeQuery("CREATE TABLE hive.hivetable.WORDS (word varchar, count INT)");
        adminExecutor.executeQuery("CREATE TABLE hive.hivetable.WORDS2 (word varchar, count INT)");

        // Now try to read it as the "public" group

        // "words" should work
        charlieExecutor.executeQuery("SELECT * FROM hive.hivetable.WORDS");
        charlieExecutor.executeQuery("select count from hive.hivetable.WORDS");

        try {
            // "words2" should not
            aliceExecutor.executeQuery("SELECT * FROM hive.hivetable.WORDS2");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }

        try {
            // "words2" should not
            aliceExecutor.executeQuery("SELECT count FROM hive.hivetable.WORDS2");
            Assert.fail("Failure expected on an unauthorized call");
        }
        catch (QueryExecutionException ex) {
            if (!(ex.getCause() instanceof SQLException && (ex.getCause()).getCause().toString().startsWith(AccessDeniedException.class.getCanonicalName()))) {
                throw ex;
            }
        }

        // Drop the table and database as "admin"
        adminExecutor.executeQuery("drop TABLE hive.hivetable.WORDS");
        adminExecutor.executeQuery("drop TABLE hive.hivetable.WORDS2");
        adminExecutor.executeQuery("drop DATABASE hive.hivetable");
    }
}

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
package snowflake;

import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.assertions.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Test(singleThreaded=true)
public class TestSnowflakeQueries
{
    public static final String ACCESS_DENIED = "Access Denied: Cannot access catalog snowflake";
    private final Session datapipesSession = sessionOf("datapipes@cldcvr.com");
    private final Session devSession = sessionOf("dev@cldcvr.com");
    private DistributedQueryRunner queryRunner;

    public static Session sessionOf(String user)
    {
        Session session = SnowflakeSqlQueryRunner.createSession();
        return Session.builder(session)
                .setIdentity(Identity.ofUser(user))
                .setCatalogSessionProperty("snowflake",
                        SnowflakeSessionPropertiesProvider.WRITE_FORMAT
                        , "JDBC").build();
    }

    @BeforeTest
    public void setup()
    {
        SnowflakeSqlQueryRunner.setup();
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner.close();
        }
        queryRunner = null;
    }

    @Test
    public void testAccessForGrantedUser()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_catalog_only.json");
        MaterializedResult showSchemas = queryRunner.execute(datapipesSession, "SHOW SCHEMAS FROM snowflake");
        MaterializedResult showCatalogs = queryRunner.execute(datapipesSession, "SHOW CATALOGS");
        Assert.assertTrue(showSchemas.getRowCount() > 1);
        Assert.assertTrue(toStream(showSchemas).collect(Collectors.toSet())
                .contains(SnowflakeSqlQueryRunner.getSchema()));
        Assert.assertEquals(showCatalogs.getRowCount(), 1);
        Assert.assertTrue(toStream(showCatalogs).collect(Collectors.toSet())
                .contains("snowflake"));
    }

    @AfterMethod
    public void closeQueryRunner()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner.close();
        }
        queryRunner = null;
    }

    @Test
    public void testAccessForGrantedUserNegative()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_catalog_only.json");
        //For dev@cldcvr.com we should not get any schemas
        assertAccessDenied(devSession, "SHOW SCHEMAS FROM snowflake", ACCESS_DENIED);
        assertAccessDenied(devSession, "SHOW CATALOGS", ACCESS_DENIED);
    }

    private DistributedQueryRunner getQueryRunner(String file)
            throws Exception
    {
        return SnowflakeSqlQueryRunner
                .createSnowflakeSqlQueryRunner(SnowflakeSqlQueryRunner.getCatalog(),
                        SnowflakeSqlQueryRunner.getWarehouse(),
                        Optional.of(file));
    }

    private Stream<Object> toStream(MaterializedResult rows)
    {
        return rows.getMaterializedRows().stream().map(f -> f.getFields()
                .get(0));
    }

    @Test
    public void testAccessJunitTablesDev()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_tables_only.json");
        assertAccessDenied(devSession, "SHOW TABLES FROM snowflake.junit", ACCESS_DENIED);
    }

    @Test
    public void testAccessPublicTables()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_tables_only.json");
        MaterializedResult publicTables = queryRunner.execute(datapipesSession, "SHOW TABLES FROM snowflake.public");
        System.out.println(publicTables);
        Set<Object> outputJunitTables = toStream(publicTables).collect(Collectors.toSet());
        Assert.assertEquals(outputJunitTables.size(), 0);
    }

    @Test
    public void testAccessJunitTables()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_tables_only.json");
        MaterializedResult junitTables = queryRunner.execute(datapipesSession, "SHOW TABLES FROM snowflake.junit");
        System.out.println(junitTables);
        Set<Object> outputJunitTables = toStream(junitTables).collect(Collectors.toSet());
        Assert.assertEquals(outputJunitTables.size(), 1);
        Assert.assertTrue(outputJunitTables.contains("testing_suite"));
    }

    @Test
    public void testAccessForSchemaOnlyDevSession()
            throws Exception
    {
        queryRunner = getQueryRunner("allow_tables_only.json");
        assertAccessDenied(devSession, "SHOW SCHEMAS FROM snowflake", ACCESS_DENIED);
    }

    @Test
    public void testRowFilterForDatapipes()
            throws Exception
    {
        queryRunner = getQueryRunner("row_filter_rules.json");
        MaterializedResult query = queryRunner.execute(datapipesSession, "SELECT id from snowflake.junit.testing_suite");
        Set<Integer> output = toStream(query).map(f -> Integer.parseInt(f + "")).collect(Collectors.toSet());
        Set<Integer> expected = IntStream.range(0, 100)
                .boxed()
                .collect(Collectors.toSet());
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testRowFilterForDev()
            throws Exception
    {
        queryRunner = getQueryRunner("row_filter_rules.json");
        MaterializedResult query = queryRunner.execute(devSession, "SELECT id from snowflake.junit.testing_suite");
        Set<Integer> output = toStream(query).map(f -> Integer.parseInt(f + "")).collect(Collectors.toSet());
        Set<Integer> expected = IntStream.range(6, 11)
                .boxed()
                .collect(Collectors.toSet());
        System.out.println(output);
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testColumnRestrictionSelectStarDevUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_restriction_rules.json");
        assertAccessDenied(devSession, "SELECT * from snowflake.junit.testing_suite", "Access Denied: Cannot select from table snowflake.junit.testing_suite");
    }

    @Test
    public void testColumnRestrictionSelectIdDevUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_restriction_rules.json");
        MaterializedResult query = queryRunner.execute(devSession, "SELECT id from snowflake.junit.testing_suite");
        Set<Integer> output = toStream(query).map(f -> Integer.parseInt(f + "")).collect(Collectors.toSet());
        Set<Integer> expected = IntStream.range(0, 100)
                .boxed()
                .collect(Collectors.toSet());
        System.out.println(output);
        Assert.assertEquals(expected, output);
    }

    @Test
    public void testColumnRestrictionSelectStarDatapipesUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_restriction_rules.json");
        MaterializedResult query = queryRunner.execute(datapipesSession, "SELECT * from snowflake.junit.testing_suite");
        List<Integer> idOutput = query.getMaterializedRows().stream().map(f -> f.getField(0))
                .map(f -> Integer.parseInt(f + ""))
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
        Set<String> nameOutput = query.getMaterializedRows().stream().map(f -> f.getField(1))
                .map(f -> f + "")
                .collect(Collectors.toSet());
        Set<Integer> expected = IntStream.range(0, 100)
                .boxed()
                .collect(Collectors.toSet());
        Assert.assertEquals(expected, idOutput);
        nameOutput.forEach(f -> {
            if (f == null) {
                Assert.fail("Names should not have a null value");
            }
        });
    }

    @Test
    public void testColumnMaskingAndRowFilterSelectStarDevUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_access_and_row_filter_rules.json");
        assertAccessDenied(devSession, "SELECT * from snowflake.public.testing_suite_1",
                "Access Denied: Cannot select from table snowflake.public.testing_suite_1");
    }

    @Test
    public void testColumnMaskingAndRowFilterSelectIdDevUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_access_and_row_filter_rules.json");
        //as per line "filter": "id <=10" we should get only rows till 10
        MaterializedResult outputRows = queryRunner.execute(devSession, "SELECT id from snowflake.public.testing_suite_1");
        System.out.println("RC " + outputRows.getRowCount());
        Set<Integer> output = toStream(outputRows)
                .map(f -> Integer.parseInt(f.toString()))
                .sorted()
                .collect(Collectors.toSet());
        Set<Integer> expectedOutput = IntStream.range(0, 51)
                .boxed().sorted().collect(Collectors.toSet());
        Assert.assertEquals(output, expectedOutput);
    }

    @Test
    public void testColumnMaskingAndRowFilterSelectIdAndNameDevUser()
            throws Exception
    {
        queryRunner = getQueryRunner("column_access_and_row_filter_rules.json");
        //as per line "filter": "id <=50" we should get only rows till 50
        MaterializedResult outputRows = queryRunner.execute(devSession, "SELECT id,country from snowflake.public.testing_suite_1");
        Assert.assertEquals(outputRows.getRowCount(), 51);
        Map<Integer, String> output = outputRows.getMaterializedRows()
                .stream()
                .collect(Collectors.toMap(f -> Integer.parseInt(f.getField(0).toString()),
                        f -> f.getField(1).toString()));
        IntStream.range(0, 51)
                .boxed()
                .forEach(f -> {
                    String value = output.get(f);
                    Assert.assertTrue(Arrays.asList("IN", "SG", "XX").contains(value));
                });
    }

    private void assertAccessDenied(Session session, String sql, String expectedErrorMessage)
    {
        try {
            MaterializedResult query = queryRunner.execute(session, sql);
            Assert.fail("Should not allow query: [" + sql + "] to be executed for user: [" + session.getUser() + "]");
        }
        catch (Exception e) {
            Assert.assertEquals(QueryFailedException.class, e.getClass());
            Assert.assertEquals(e.getMessage(), expectedErrorMessage);
        }
    }
    @Test
    public void test1()
            throws Exception
    {
        queryRunner = getQueryRunner("column_access_and_row_filter_rules.json");
        //as per line "filter": "id <=50" we should get only rows till 50
        //MaterializedResult outputRows = queryRunner.execute(devSession, "SELECT id from snowflake.public.testing_suite_1");
        MaterializedResult outputRows = queryRunner.execute(devSession, "desc snowflake.public.testing_suite_1");
        System.out.println("++++  "+outputRows);
        outputRows.getMaterializedRows()
                .stream()
                .forEach(System.out::println);
    }
}

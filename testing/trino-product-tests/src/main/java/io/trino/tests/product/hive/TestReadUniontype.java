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
package io.trino.tests.product.hive;

import io.trino.jdbc.Row;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.query.QueryResult;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestReadUniontype
        extends HiveProductTest
{
    private static final String TABLE_NAME = "test_read_uniontype";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void cleanup()
    {
        onHive().executeQuery(format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    }

    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new String[][] {{"ORC"}, {"AVRO"}};
    }

    private void createTestTable(String storageFormat)
    {
        cleanup();
        onHive().executeQuery(format(
                "CREATE TABLE %s (id INT,foo UNIONTYPE<" +
                        "INT," +
                        "DOUBLE," +
                        "ARRAY<STRING>>)" +
                        "STORED AS %s",
                TABLE_NAME,
                storageFormat));
    }

    @Test(dataProvider = "storage_formats", groups = SMOKE)
    public void testReadUniontype(String storageFormat)
    {
        // According to testing results, the Hive INSERT queries here only work in Hive 1.2
        if (getHiveVersionMajor() != 1 || getHiveVersionMinor() != 2) {
            throw new SkipException("This test can only be run with Hive 1.2 (default config)");
        }
        createTestTable(storageFormat);
        // Generate a file with rows:
        //   0, {0: 36}
        //   1, {1: 7.2}
        //   2, {2: ['foo', 'bar']}
        //   3, {1: 10.8}
        //   4, {0: 144}
        //   5, {2: ['hello']
        onHive().executeQuery(format(
                "INSERT INTO TABLE %s " +
                        "SELECT 0, create_union(0, CAST(36 AS INT), CAST(NULL AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 1, create_union(1, CAST(NULL AS INT), CAST(7.2 AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 2, create_union(2, CAST(NULL AS INT), CAST(NULL AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 3, create_union(1, CAST(NULL AS INT), CAST(10.8 AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 4, create_union(0, CAST(144 AS INT), CAST(NULL AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 5, create_union(2, CAST(NULL AS INT), CAST(NULL AS DOUBLE), ARRAY('hello', 'world'))",
                TABLE_NAME));
        // Generate a file with rows:
        //    6, {0: 180}
        //    7, {1: 21.6}
        //    8, {0: 252}
        onHive().executeQuery(format(
                "INSERT INTO TABLE %s " +
                        "SELECT 6, create_union(0, CAST(180 AS INT), CAST(NULL AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 7, create_union(1, CAST(NULL AS INT), CAST(21.6 AS DOUBLE), ARRAY('foo','bar')) " +
                        "UNION ALL " +
                        "SELECT 8, create_union(0, CAST(252 AS INT), CAST(NULL AS DOUBLE), ARRAY('foo','bar'))",
                TABLE_NAME));
        QueryResult selectAllResult = onTrino().executeQuery(format("SELECT * FROM %s", TABLE_NAME));
        assertEquals(selectAllResult.rows().size(), 9);
        for (List<?> row : selectAllResult.rows()) {
            int id = (Integer) row.get(0);
            switch (id) {
                case 0:
                    assertStructEquals(row.get(1), new Object[] {(byte) 0, 36, null, null});
                    break;
                case 1:
                    assertStructEquals(row.get(1), new Object[] {(byte) 1, null, 7.2D, null});
                    break;
                case 2:
                    assertStructEquals(row.get(1), new Object[] {(byte) 2, null, null, Arrays.asList("foo", "bar")});
                    break;
                case 3:
                    assertStructEquals(row.get(1), new Object[] {(byte) 1, null, 10.8D, null});
                    break;
                case 4:
                    assertStructEquals(row.get(1), new Object[] {(byte) 0, 144, null, null});
                    break;
                case 5:
                    assertStructEquals(row.get(1), new Object[] {(byte) 2, null, null, Arrays.asList("hello", "world")});
                    break;
                case 6:
                    assertStructEquals(row.get(1), new Object[] {(byte) 0, 180, null, null});
                    break;
                case 7:
                    assertStructEquals(row.get(1), new Object[] {(byte) 1, null, 21.6, null});
                    break;
                case 8:
                    assertStructEquals(row.get(1), new Object[] {(byte) 0, 252, null, null});
                    break;
            }
        }
    }

    // TODO use Row as expected too, and use tempto QueryAssert
    private static void assertStructEquals(Object actual, Object[] expected)
    {
        assertThat(actual).isInstanceOf(Row.class);
        Row actualRow = (Row) actual;
        assertEquals(actualRow.getFields().size(), expected.length);
        for (int i = 0; i < actualRow.getFields().size(); i++) {
            assertEquals(actualRow.getFields().get(i).getValue(), expected[i]);
        }
    }
}

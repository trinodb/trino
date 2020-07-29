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
package io.prestosql.tests.iceberg;

import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.ICEBERG;
import static io.prestosql.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static io.prestosql.tests.utils.QueryExecutors.onSpark;
import static java.lang.String.format;

public class TestIcebergBasic
        extends ProductTest
{
    // see spark-defaults.conf
    private static final String SPARK_CATALOG = "iceberg_test";
    private static final String PRESTO_CATALOG = "iceberg";
    private static final String TABLE_NAME = "test_iceberg_basic";
    private static final String SPARK_TABLE_NAME = format("%s.default.%s", SPARK_CATALOG, TABLE_NAME);
    private static final String PRESTO_TABLE_NAME = format("%s.default.%s", PRESTO_CATALOG, TABLE_NAME);

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTables()
    {
        onPresto().executeQuery(format("DROP TABLE IF EXISTS %s", PRESTO_TABLE_NAME));
    }

    @Test(groups = {ICEBERG, PROFILE_SPECIFIC_TESTS})
    public void testCompatibility()
    {
        onSpark().executeQuery(format("CREATE TABLE %s (id BIGINT) USING ICEBERG", SPARK_TABLE_NAME));
        onSpark().executeQuery(format("INSERT INTO %s VALUES (42)", SPARK_TABLE_NAME));

        QueryResult sparkQueryResult = onSpark().executeQuery(format("SELECT * FROM %s", SPARK_TABLE_NAME));
        assertThat(sparkQueryResult).containsOnly(row(42L));

        QueryResult prestoQueryResult = onPresto().executeQuery(format("SELECT * FROM %s", PRESTO_TABLE_NAME));
        assertThat(prestoQueryResult).containsOnly(row(42L));
    }
}

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
package io.trino.tests.product.mysql;

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.MYSQL;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onMySql;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcDynamicFilteringJmx
        extends ProductTest
{
    private static final String TABLE_NAME = "test.nation_tmp";

    @BeforeMethodWithContext
    @AfterMethodWithContext
    public void dropTestTable()
    {
        onMySql().executeQuery(format("DROP TABLE IF EXISTS %s", TABLE_NAME));
    }

    @Test(groups = {MYSQL, PROFILE_SPECIFIC_TESTS})
    public void testDynamicFilteringStats()
    {
        assertThat(onTrino().executeQuery(format("CREATE TABLE mysql.%s AS SELECT * FROM tpch.tiny.nation", TABLE_NAME)))
                .containsOnly(row(25));

        onTrino().executeQuery("SET SESSION mysql.dynamic_filtering_wait_timeout = '1h'");
        onTrino().executeQuery("SET SESSION join_reordering_strategy = 'NONE'");
        onTrino().executeQuery("SET SESSION join_distribution_type = 'BROADCAST'");
        assertThat(onTrino().executeQuery(format("SELECT COUNT(*) FROM mysql.%s a JOIN tpch.tiny.nation b ON a.nationkey = b.nationkey AND b.name = 'INDIA'", TABLE_NAME)))
                .containsOnly(row(1));

        assertThat(onTrino().executeQuery("SELECT \"completeddynamicfilters.totalcount\" FROM jmx.current.\"io.trino.plugin.jdbc:name=mysql,type=dynamicfilteringstats\" WHERE node = 'presto-master'"))
                .containsOnly(row(1));
        assertThat(onTrino().executeQuery("SELECT \"totaldynamicfilters.totalcount\" FROM jmx.current.\"io.trino.plugin.jdbc:name=mysql,type=dynamicfilteringstats\" WHERE node = 'presto-master'"))
                .containsOnly(row(1));
    }
}

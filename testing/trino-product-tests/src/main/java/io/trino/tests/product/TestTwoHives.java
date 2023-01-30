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
package io.trino.tests.product;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.TWO_HIVES;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.Math.pow;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestTwoHives
{
    private static final List<String> CATALOGS = ImmutableList.of("hive1", "hive2");

    @Test(groups = {TWO_HIVES, PROFILE_SPECIFIC_TESTS}, dataProvider = "catalogs")
    public void testCreateTableAsSelectAndAnalyze(String catalog)
    {
        onTrino().executeQuery(format("DROP TABLE IF EXISTS %s.default.nation", catalog));

        assertThat(onTrino().executeQuery(format("CREATE TABLE %s.default.nation AS SELECT * FROM tpch.tiny.nation", catalog)))
                .containsOnly(row(25));

        assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s.default.nation", catalog)))
                .containsOnly(row(25));

        onTrino().executeQuery(format("ANALYZE %s.default.nation", catalog));

        assertThat(onTrino().executeQuery(format("SHOW STATS FOR %s.default.nation", catalog)))
                .containsOnly(
                        row("nationkey", null, 25.0, 0.0, null, "0", "24"),
                        row("name", 177.0, 25.0, 0.0, null, null, null),
                        row("regionkey", null, 5.0, 0.0, null, "0", "4"),
                        row("comment", 1857.0, 25.0, 0.0, null, null, null),
                        row(null, null, null, null, 25.0, null, null));

        onTrino().executeQuery(format("DROP TABLE %s.default.nation", catalog));
    }

    @Test(groups = {TWO_HIVES, PROFILE_SPECIFIC_TESTS})
    public void testJoinFromTwoHives()
    {
        CATALOGS.forEach(catalog -> {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s.default.nation", catalog));
            assertThat(onTrino().executeQuery(format("CREATE TABLE %s.default.nation AS SELECT * FROM tpch.tiny.nation", catalog)))
                    .containsOnly(row(25));
        });

        assertThat(onTrino().executeQuery(format(
                "SELECT count(*) FROM %s",
                CATALOGS.stream()
                        .map(catalog -> format("%s.default.nation", catalog))
                        .collect(joining(",")))))
                .containsOnly(row((int) pow(25, CATALOGS.size())));

        CATALOGS.forEach(catalog -> onTrino().executeQuery(format("DROP TABLE %s.default.nation", catalog)));
    }

    @DataProvider(name = "catalogs")
    public static Object[][] catalogs()
    {
        return CATALOGS.stream()
                .map(catalog -> new Object[] {catalog})
                .toArray(Object[][]::new);
    }
}

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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.RequiresEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.Math.pow;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

@ProductTest
@RequiresEnvironment(ProductTestEnvironment.class)
@TestGroup.TwoHives
@TestGroup.ProfileSpecificTests
class TestTwoHivesJunit
{
    private static final List<String> CATALOGS = List.of("hive1", "hive2");

    @ParameterizedTest
    @ValueSource(strings = {"hive1", "hive2"})
    void testCreateTableAsSelectAndAnalyze(String catalog, ProductTestEnvironment env)
    {
        env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s.default.nation", catalog));

        Assertions.assertEquals(25, env.executeTrinoUpdate(format("CREATE TABLE %s.default.nation AS SELECT * FROM tpch.tiny.nation", catalog)));

        assertThat(env.executeTrino(format("SELECT count(*) FROM %s.default.nation", catalog)))
                .containsOnly(row(25));

        env.executeTrinoUpdate(format("ANALYZE %s.default.nation", catalog));

        assertThat(env.executeTrino(format("SHOW STATS FOR %s.default.nation", catalog)))
                .containsOnly(
                        row("nationkey", null, 25.0, 0.0, null, "0", "24"),
                        row("name", 177.0, 25.0, 0.0, null, null, null),
                        row("regionkey", null, 5.0, 0.0, null, "0", "4"),
                        row("comment", 1857.0, 25.0, 0.0, null, null, null),
                        row(null, null, null, null, 25.0, null, null));

        env.executeTrinoUpdate(format("DROP TABLE %s.default.nation", catalog));
    }

    @Test
    void testJoinFromTwoHives(ProductTestEnvironment env)
    {
        CATALOGS.forEach(catalog -> {
            env.executeTrinoUpdate(format("DROP TABLE IF EXISTS %s.default.nation", catalog));
            Assertions.assertEquals(25, env.executeTrinoUpdate(format("CREATE TABLE %s.default.nation AS SELECT * FROM tpch.tiny.nation", catalog)));
        });

        assertThat(env.executeTrino(format(
                "SELECT count(*) FROM %s",
                CATALOGS.stream()
                        .map(catalog -> format("%s.default.nation", catalog))
                        .collect(joining(",")))))
                .containsOnly(row((int) pow(25, CATALOGS.size())));

        CATALOGS.forEach(catalog -> env.executeTrinoUpdate(format("DROP TABLE %s.default.nation", catalog)));
    }
}

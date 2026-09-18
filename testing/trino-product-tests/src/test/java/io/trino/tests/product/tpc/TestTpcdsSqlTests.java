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
package io.trino.tests.product.tpc;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.SqlTestRunner;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.tests.product.ConfiguredFeatures.assertDefaultConnectors;

@ProductTest
@RequiresEnvironment(TpcdsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Tpcds
class TestTpcdsSqlTests
{
    @Test
    void testConfiguredConnectors(TpcdsEnvironment environment)
    {
        assertDefaultConnectors(environment, "hive");
    }

    @ParameterizedTest
    @MethodSource("queries")
    void testQuery(String queryId, TpcdsEnvironment environment)
            throws IOException
    {
        SqlTestRunner.run(environment, "tpcds/q" + queryId);
    }

    private static Stream<String> queries()
    {
        Set<Integer> queriesWithVariants = Set.of(14, 23, 24, 39);
        return Stream.concat(
                        IntStream.rangeClosed(1, 99)
                                .filter(query -> !queriesWithVariants.contains(query))
                                .mapToObj(query -> "%02d".formatted(query)),
                        Stream.of("14_1", "14_2", "23_1", "23_2", "24_1", "24_2", "39_1", "39_2"))
                .sorted();
    }
}

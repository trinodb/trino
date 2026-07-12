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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@ProductTest
@RequiresEnvironment(TpchEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Tpch
class TestTpchSqlTests
{
    @ParameterizedTest
    @MethodSource("queries")
    void testQuery(String queryId, TpchEnvironment environment)
            throws IOException
    {
        SqlTestRunner.run(environment, "hive_tpch", queryId);
    }

    private static Stream<String> queries()
    {
        return IntStream.rangeClosed(1, 22)
                .mapToObj(query -> "%02d".formatted(query));
    }
}

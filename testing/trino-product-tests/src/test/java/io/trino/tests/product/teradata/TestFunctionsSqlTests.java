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
package io.trino.tests.product.teradata;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.SqlTestRunner;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

@ProductTest
@RequiresEnvironment(FunctionsEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.Functions
class TestFunctionsSqlTests
{
    @ParameterizedTest
    @MethodSource("testCases")
    void testFunctionsRegistered(String testCase, FunctionsEnvironment environment)
            throws IOException
    {
        SqlTestRunner.run(environment, testCase);
    }

    private static Stream<String> testCases()
    {
        return Stream.of(
                "array_functions/checkArrayFunctionsRegistered",
                "binary_functions/checkBinaryFunctionsRegistered",
                "horology_functions/checkHorologyFunctionsRegistered",
                "json_functions/checkJsonFunctionsRegistered",
                "map_functions/checkMapFunctionsRegistered",
                "math_functions/checkMathFunctionsRegistered",
                "regex_functions/checkRegexFunctionsRegistered",
                "string_functions/checkStringFunctionsRegistered",
                "url_functions/checkUrlFunctionsRegistered");
    }
}

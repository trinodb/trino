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
package io.trino.operator;

import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;

class TestFlatHashStrategyCompiler
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    @Test
    void test()
    {
        // this will work with 100K columns, but that uses too much memory for the CI
        FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(2_001, BIGINT), TYPE_OPERATORS);
        FlatHashStrategyCompiler.compileFlatHashStrategy(nCopies(2_001, VARCHAR), TYPE_OPERATORS);
    }
}

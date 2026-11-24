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
package io.trino.sql.planner;

import org.junit.jupiter.api.Test;

import static io.airlift.testing.EquivalenceTester.equivalenceTester;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;

class TestSymbol
{
    @Test
    void testEquivalence()
    {
        equivalenceTester()
                .addEquivalentGroup(new Symbol(BIGINT, "a"), new Symbol(BIGINT, "a"))
                .addEquivalentGroup(new Symbol(VARCHAR, "a"), new Symbol(VARCHAR, "a"))
                .addEquivalentGroup(new Symbol(createVarcharType(10), "a"), new Symbol(createVarcharType(10), "a"))
                .addEquivalentGroup(new Symbol(BIGINT, "b"), new Symbol(BIGINT, "b"))
                .check();
    }
}

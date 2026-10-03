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
package io.trino.metadata;

import io.trino.spi.type.TypeSyntax;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.tree.FunctionSpecification;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.LanguageFunctionManager.canonicalizeFunctionTypes;
import static io.trino.metadata.LanguageFunctionManager.legacySignatureType;
import static io.trino.metadata.LanguageFunctionManager.normalizeStoredFunctionTypes;
import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestStoredFunctionTypes
{
    @Test
    void testLegacyFunctionAcceptsMoreThanNinetyNineDays()
    {
        FunctionSpecification function = normalizeStoredFunctionTypes(new SqlParser().createFunctionSpecification(
                "FUNCTION f(x interval day to second) RETURNS interval day to second RETURN x"));
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query("WITH " + formatSql(function) + " SELECT CAST(f(INTERVAL '123' DAY) AS varchar)"))
                    .matches("VALUES varchar '123 00:00:00.000'");
        }
        assertThat(TypeSyntax.toSql(legacySignatureType(TESTING_TYPE_MANAGER.fromSqlType("array(interval day(9) to second(3))").getTypeDescriptor())))
                .isEqualTo("array(interval day to second)");
    }

    @Test
    void testNewFunctionDefaultsAreStoredExplicitly()
    {
        FunctionSpecification function = canonicalizeFunctionTypes(new SqlParser().createFunctionSpecification(
                "FUNCTION f(x interval day to second) RETURNS interval year to month RETURN INTERVAL '0' YEAR"));
        String sql = formatSql(function);
        assertThat(sql).containsIgnoringCase("interval day(2) to second(6)")
                .containsIgnoringCase("interval year(2) to month");
        assertThat(normalizeStoredFunctionTypes(new SqlParser().createFunctionSpecification(sql))).isEqualTo(function);
    }
}

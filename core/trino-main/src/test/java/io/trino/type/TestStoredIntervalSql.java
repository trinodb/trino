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
package io.trino.type;

import io.trino.sql.parser.SqlParser;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.SqlFormatterUtil.getFormattedSqlForStorage;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.normalizeStoredIntervalType;
import static org.assertj.core.api.Assertions.assertThat;

class TestStoredIntervalSql
{
    @Test
    void testStoredCastDefaults()
    {
        SqlParser parser = new SqlParser();
        String sql = "SELECT CAST(CAST('123 00:00:00' AS interval day to second) AS varchar)";
        String legacy = formatSql(parser.createStatement(sql, type -> normalizeStoredIntervalType(type)));
        try (QueryAssertions assertions = new QueryAssertions()) {
            assertThat(assertions.query(legacy)).matches("VALUES varchar '123 00:00:00.000'");
        }
        String stored = getFormattedSqlForStorage(parser.createStatement(sql), parser);
        assertThat(stored).containsIgnoringCase("interval day(2) to second(6)");
        assertThat(formatSql(parser.createStatement(stored, type -> normalizeStoredIntervalType(type)))).isEqualTo(stored);
    }

    @Test
    void testNestedCastAndLiteralDefaults()
    {
        SqlParser parser = new SqlParser();
        String sql = "SELECT CAST(ARRAY['123 00:00:00'] AS array(interval day to second)), INTERVAL '123' DAY";
        String legacy = formatSql(parser.createStatement(sql, type -> normalizeStoredIntervalType(type)));
        assertThat(legacy).containsIgnoringCase("interval day(9) to second(3)")
                .containsIgnoringCase("INTERVAL '123' DAY");
        assertThat(getFormattedSqlForStorage(parser.createStatement(sql), parser))
                .containsIgnoringCase("interval day(2) to second(6)")
                .containsIgnoringCase("INTERVAL '123' DAY");
    }
}

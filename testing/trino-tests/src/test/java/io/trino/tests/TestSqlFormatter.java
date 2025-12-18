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
package io.trino.tests;

import io.trino.sql.SqlFormatter;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestSqlFormatter
{
    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT +1",
            "SELECT -1",
            "SELECT + (SELECT 1)",
            "SELECT - (SELECT 1)",
    })
    public void testArithmeticUnary(String sql)
    {
        SqlParser parser = new SqlParser();
        Statement statement = parser.createStatement(sql);
        String formattedSql = SqlFormatter.formatSql(statement);
        Statement roundTripStatement = parser.createStatement(formattedSql);

        assertThat(roundTripStatement).isEqualTo(statement);
    }
}

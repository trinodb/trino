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
package io.trino.execution;

import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestParameterExtractor
{
    private final SqlParser sqlParser = new SqlParser();

    @Test
    public void testNoParameter()
    {
        Statement statement = sqlParser.createStatement("SELECT c1, c2 FROM test_table WHERE c1 = 1 AND c2 > 2", new ParsingOptions());
        assertThat(ParameterExtractor.extractParameters(statement)).isEmpty();
        assertThat(ParameterExtractor.getParameterCount(statement)).isEqualTo(0);
    }

    @Test
    public void testParameterCount()
    {
        Statement statement = sqlParser.createStatement("SELECT c1, c2 FROM test_table WHERE c1 = ? AND c2 > ?", new ParsingOptions());
        assertThat(ParameterExtractor.extractParameters(statement))
                .containsExactly(
                        new Parameter(new NodeLocation(1, 41), 0),
                        new Parameter(new NodeLocation(1, 52), 1));
        assertThat(ParameterExtractor.getParameterCount(statement)).isEqualTo(2);
    }

    @Test
    public void testShowStats()
    {
        Statement statement = sqlParser.createStatement("SHOW STATS FOR (SELECT c1, c2 FROM test_table WHERE c1 = ? AND c2 > ?)", new ParsingOptions());
        assertThat(ParameterExtractor.extractParameters(statement))
                .containsExactly(
                        new Parameter(new NodeLocation(1, 57), 0),
                        new Parameter(new NodeLocation(1, 68), 1));
        assertThat(ParameterExtractor.getParameterCount(statement)).isEqualTo(2);
    }

    @Test
    public void testLambda()
    {
        Statement statement = sqlParser.createStatement("SELECT * FROM test_table WHERE any_match(items, x -> x > ?)", new ParsingOptions());
        assertThat(ParameterExtractor.extractParameters(statement))
                .containsExactly(new Parameter(new NodeLocation(1, 58), 0));

        assertThat(ParameterExtractor.getParameterCount(statement)).isEqualTo(1);
    }
}

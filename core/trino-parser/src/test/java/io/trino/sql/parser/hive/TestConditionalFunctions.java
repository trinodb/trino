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
package io.trino.sql.parser.hive;

import org.testng.annotations.Test;

public class TestConditionalFunctions
        extends SQLTester
{
    @Test
    public void testIf()
    {
        String sql = "select IF(1=1,'TRUE','FALSE')";

        checkASTNode(sql);
    }

    @Test
    public void testNullIf()
    {
        String sql = "select nullif(a, b)";

        checkASTNode(sql);
    }

    @Test
    public void testCoalesce()
    {
        String sql = "select coalesce(a, b, c)";

        checkASTNode(sql);
    }

    @Test
    public void testCaseWhen()
    {
        String sql = "" +
                "SELECT\n" +
                "CASE   Fruit\n" +
                "       WHEN 'APPLE' THEN 'The owner is APPLE'\n" +
                "       WHEN 'ORANGE' THEN 'The owner is ORANGE'\n" +
                "       ELSE 'It is another Fruit'\n" +
                "END";

        checkASTNode(sql);
    }
}

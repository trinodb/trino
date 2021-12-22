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

public class TestLateralView
        extends SQLTester
{
    @Test
    public void testLateralView()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) AS event (c1)";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view explode(events) event as c1";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testLateralViewWithOrdinality()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (pos, c1)";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view posexplode(events) event as pos, c1";

        checkASTNode(prestoSql, hiveSql);
    }
}

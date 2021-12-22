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

import io.trino.sql.tree.Node;
import org.testng.annotations.Test;

public class TestCasesFromFile
        extends SQLTester
{
    @Test
    public void testCase01()
    {
        runHiveSQLFromFile("hive/parser/cases/case1.sql");
    }

    @Test
    public void testCase02()
    {
        checkASTNodeFromFile("hive/parser/cases/lateral-presto-1.sql",
                "hive/parser/cases/lateral-hive-1.sql");
    }

    @Test
    public void testCase03()
    {
        checkASTNodeFromFile("hive/parser/cases/lateral-presto-2.sql",
                "hive/parser/cases/lateral-hive-2.sql");
    }

    @Test
    public void testCase100()
    {
        Node node = runHiveSQLFromFile("hive/parser/cases/case100.sql");
    }
}

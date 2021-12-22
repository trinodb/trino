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

/**
 * @author tangyun@bigo.sg
 * @date 8/13/19 4:36 PM
 */
public class TestOnlineCases
        extends SQLTester
{
    @Test
    public void test02()
    {
        checkASTNodeFromFile("hive/parser/cases/from-multi-table-presto-01.sql",
                "hive/parser/cases/from-multi-table-hive-01.sql");
    }

    @Test
    public void test03()
    {
        checkASTNodeFromFile("hive/parser/cases/online-case-04.sql");
    }
}

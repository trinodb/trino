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
import io.trino.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author tangyun@bigo.sg
 * @date 12/24/19 2:52 PM
 */
public class TestReplaceSlash
        extends SQLTester
{
    @Test
    public void testReplaceRlikeSlash()
    {
        checkString("hive/parser/cases/slash-test.sql");
    }

    @Test
    public void testReplaceFunctionParaSlash()
    {
        checkString("hive/parser/cases/slash-test-function.sql");
    }

    @Test
    public void testReplaceFunctionParaSlash1()
    {
        checkString("hive/parser/cases/slash-test-function1.sql");
    }

    protected void checkString(String path)
    {
        String[] hiveSql = getResourceContent(path)
                .replace("\n", "")
                .split(";");
        Node query = runHiveSQL(hiveSql[0]);
        traversalAstTree(query, node -> {
            if (node instanceof StringLiteral) {
                assertEquals(node.toString(), hiveSql[1]);
            }
        });
    }
}

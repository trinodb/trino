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
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

/**
 * @author tangyun@bigo.sg
 * @date 12/19/19 6:19 PM
 */
public class TestCreateTableLike
        extends SQLTester
{
    @Test
    public void test()
    {
        String sql = "create table if not exists a like b";
        Node node = runHiveSQL(sql);
        QualifiedName a = QualifiedName.of("a");
        QualifiedName b = QualifiedName.of("b");

        CreateTableLike createTableLike = new CreateTableLike(
                Optional.empty(),
                a,
                b,
                true);
        checkASTNode(node, createTableLike);
    }
}

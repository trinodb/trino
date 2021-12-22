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

import io.trino.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class TestInsert
        extends SQLTester
{
    @Test
    public void insertIntoSelect()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-into-select-hive.sql");
    }

    @Test
    public void insertIntoSelect1()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-into-select-hive1.sql");
    }

    @Test
    public void insertIntoSelect2()
    {
        String prestoSql = "insert overwrite t select a from t1 union select b from t2";
        String hiveSql = "insert overwrite table t select a from t1 union select b from t2";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void insertOverwriteSelect()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto.sql",
                "hive/parser/cases/insert-overwrite-select-hive.sql");
    }

    @Test
    public void insertOverwriteSelect1()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto.sql",
                "hive/parser/cases/insert-overwrite-select-hive1.sql");
    }

    @Test
    public void insertOverwriteSelect2()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto2.sql",
                "hive/parser/cases/insert-overwrite-select-hive2.sql");
    }

    @Test
    public void insertOverwriteSelect3()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto4.sql",
                "hive/parser/cases/insert-overwrite-select-hive4.sql");
    }

    @Test
    public void insertOverwriteSelectOnlineCase()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-overwrite-select-presto3.sql",
                "hive/parser/cases/insert-overwrite-select-hive3.sql");
    }

    @Test(expectedExceptions = ParsingException.class)
    public void insertOverwriteSelect4()
    {
        runHiveSQLFromFile("hive/parser/cases/insert-overwrite-select-hive5.sql");
    }

    @Test
    public void insertIntoPartitionSelect1()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-into-partition-select-presto.sql",
                "hive/parser/cases/insert-into-partition-select-hive1.sql");
    }

    @Test
    public void insertIntoPartitionSelect2()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-into-partition-select-presto2.sql",
                "hive/parser/cases/insert-into-partition-select-hive2.sql");
    }

    @Test
    public void insertIntoPartitionSelect3()
    {
        checkASTNodeFromFile("hive/parser/cases/insert-into-partition-select-presto4.sql",
                "hive/parser/cases/insert-into-partition-select-hive4.sql");
    }
}

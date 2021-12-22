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

public class TestUnsupportedSQLs
        extends SQLTester
{
    @Test(expectedExceptions = ParsingException.class)
    public void sortByShouldThrowException()
    {
        String sql = "SELECT a from b sort by c";
        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void listResourceThrowException()
    {
        String sql = "LIST FILES";
        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void distinctOnThrowException()
    {
        String sql = "SELECT distinct on a from tb1";

        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void missingSelectStatementShouldThrowException()
    {
        String sql = "from tb1 where a > 10";

        runHiveSQL(sql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void loadDataShouldThrowException()
    {
        String sql = "load data inpath '/directory-path/file.csv' into tbl";

        runHiveSQL(sql);
    }

    // create table as select:when no as
    @Test(expectedExceptions = ParsingException.class)
    public void testCase10()
    {
        String hiveSql = "create table t select m from t1";
        checkASTNode(hiveSql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void testCase11()
    {
        String hiveSql = "select count() from tbl";
        checkASTNode(hiveSql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void testCase12()
    {
        String hiveSql = "alter table tbl change c d string";
        runHiveSQL(hiveSql);
    }

    @Test(expectedExceptions = ParsingException.class)
    public void testCase13()
    {
        String hiveSql = "alter table tbl change c string";
        runHiveSQL(hiveSql);
    }
}

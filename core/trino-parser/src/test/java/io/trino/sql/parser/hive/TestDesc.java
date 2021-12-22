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
 * @date 8/15/19 2:12 PM
 */
public class TestDesc
        extends SQLTester
{
    @Test
    public void test01()
    {
        String sql = "desc tablename";
        checkASTNode(sql);
    }

    @Test
    public void test02()
    {
        String hiveSql = "desc a.tablename";
        String prestoSql = "desc a.tablename";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test03()
    {
        String hiveSql = "desc table a.tablename";
        String prestoSql = "desc a.tablename";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test06()
    {
        String hiveSql = "show columns from a.tablename";
        checkASTNode(hiveSql);
    }

    @Test
    public void test07()
    {
        String hiveSql = "show columns in tablename";
        checkASTNode(hiveSql);
    }
}

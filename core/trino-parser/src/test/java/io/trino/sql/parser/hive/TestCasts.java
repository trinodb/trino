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

public class TestCasts
        extends SQLTester
{
    @Test
    public void testCast()
    {
        String sql = "SELECT cast ('1' as INT)";

        checkASTNode(sql);
    }

    @Test
    public void testCastArray()
    {
        String sql = " SELECT CAST(1 AS ARRAY<INT>)";

        checkASTNode(sql);
    }

    @Test
    public void testCastMap()
    {
        String sql = "SELECT cast ('1' as MAP<INT,BIGINT>)";

        checkASTNode(sql);
    }

    @Test
    public void testCastStruct()
    {
        String prestoSql = "SELECT CAST(1 AS ROW(x BIGINT, y VARCHAR))";
        String hiveSql = "SELECT CAST(1 AS STRUCT<x: BIGINT, y: VARCHAR>)";

        checkASTNode(prestoSql, hiveSql);
    }
}

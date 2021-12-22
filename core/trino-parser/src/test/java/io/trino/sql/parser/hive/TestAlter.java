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
 * @date 8/13/19 3:14 PM
 */
public class TestAlter
        extends SQLTester
{
    @Test
    public void testAlterDropPartition01()
    {
        String prestoSql = "delete from tbl where day = '2019-05-01' and hour = '01'";
        String hiveSql = "ALTER TABLE tbl DROP IF EXISTS PARTITION (day = '2019-05-01', hour='01')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testAlterDropPartition02()
    {
        String prestoSql = "delete from tbl where (day = '2019-05-01' and hour = '01') or (day = '2019-05-01' and hour = '02')";
        String hiveSql = "ALTER TABLE tbl DROP IF EXISTS PARTITION (day = '2019-05-01', hour='01'),PARTITION (day = '2019-05-01', hour='02')";
        checkASTNode(prestoSql, hiveSql);
    }
}

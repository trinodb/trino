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
 * @date 10/14/19 2:28 PM
 */
public class TestAlterTable
        extends SQLTester
{
    @Test
    public void test01()
    {
        String prestoSql = "alter table tbl add column \"col\" BIGINT comment 'c'";
        String hiveSql = "alter table tbl add columns (col BIGINT comment 'c')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test02()
    {
        String prestoSql = "alter table tbl add column \"col\" ARRAY(ROW(S BIGINT)) comment 'c'";
        String hiveSql = "alter table tbl add columns (col ARRAY<STRUCT<S:BIGINT>> comment 'c')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test03()
    {
        String prestoSql = "alter table tbl rename to tbl1";
        checkASTNode(prestoSql);
    }

    @Test
    public void test04()
    {
        String prestoSql = "alter table tbl add column event ROW(time bigint, lng bigint, lat bigint, net string, log_extra map(string, string), event_id string, event_info map(string, string)) COMMENT 'from deserializer'";
        String hiveSql = "alter table tbl add columns( `event` struct<time:bigint,lng:bigint,lat:bigint,net:string,log_extra:map<string,string>,event_id:string,event_info:map<string,string>> COMMENT 'from deserializer')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void test05()
    {
        String prestoSql = "alter table tbl add column event ROW(time bigint, lng bigint, lat bigint, net string, log_extra map(string, string), event_id string, event_info map(string, string)) COMMENT 'from deserializer'";
        String hiveSql = "alter table tbl add columns( `event` struct<`time`:bigint,lng:bigint,lat:bigint,net:string,`log_extra`:map<string,string>,event_id:string,event_info:map<string,string>> COMMENT 'from deserializer')";
        checkASTNode(prestoSql, hiveSql);
    }
}

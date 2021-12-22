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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestParseErrorMessages
        extends SQLTester
{
    @Test
    public void testAntlr4ParseErrorMessage()
    {
        String sql = "SELECT a from b from c";

        try {
            runHiveSQL(sql);
            fail("sql: " + sql + " should throw exception");
        }
        catch (ParsingException e) {
        }
    }

    @Test
    public void testInternalParseErrorMessage()
    {
        String sql = "SELECT a from b sort by c";

        try {
            runHiveSQL(sql);
            fail("sql: " + sql + " should throw exception");
        }
        catch (ParsingException e) {
            assertTrue(e.getMessage().contains("Don't support sort by"));
        }
    }

    @Test
    public void testUsingUDTFFuncCall()
    {
        String sql = "" +
                "select explode(`tables`) as `tables` from bigolive.hive_job_audit\n" +
                "where day >= '2019-06-01'";

        try {
            runHiveSQL(sql);
            fail("sql: " + sql + " should throw exception");
        }
        catch (ParsingException e) {
            assertTrue(e.getMessage().contains("Don't Support call UDTF: explode directly, please try lateral view syntax instead."));
        }
    }

    @Test
    public void testCrossJoinUnnestInHiveMode()
    {
        String sql = "" +
                "select  day,hour,count(distinct a.UID) as uv\n" +
                "from\n" +
                "(\n" +
                "SELECT day,hour(rtime) as hour,uid1 as uid\n" +
                "from vlog.like_online_user_uversion_stat_platform\n" +
                "cross join unnest(online) as ont\n" +
                "cross join unnest(ont.uids) as ut  (uid1)\n" +
                "where day>='2019-08-13'\n" +
                "and status=0\n" +
                ") a\n" +
                "join\n" +
                "(\n" +
                "  select UID\n" +
                "  from vlog.user_countrycode\n" +
                "  where countrycode='BD'\n" +
                ")b\n" +
                "on a.uid=b.uid\n" +
                "group by day,hour" +
                "";

        try {
            runHiveSQL(sql);
            fail("sql: " + sql + " should throw exception");
        }
        catch (ParsingException e) {
            assertTrue(e.getMessage().contains("Don't support unnest"));
        }
    }
}

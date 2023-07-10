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
package io.trino.plugin.mysql;

import org.testng.SkipException;

public class TestMySqlTableStatisticsMySql8IndexStatistics
        extends BaseMySqlTableStatisticsIndexStatisticsTest
{
    public TestMySqlTableStatisticsMySql8IndexStatistics()
    {
        super("mysql:8.0.30");
    }

    @Override
    public void testNotAnalyzed()
    {
        throw new SkipException("MySql8 automatically calculates stats - https://dev.mysql.com/doc/refman/8.0/en/innodb-parameters.html#sysvar_innodb_stats_auto_recalc");
    }
}

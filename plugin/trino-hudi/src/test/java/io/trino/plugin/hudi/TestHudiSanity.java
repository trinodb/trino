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

package io.trino.plugin.hudi;

import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestHudiSanity
        extends AbstractHudiTestQueryFramework
{
    // TODO: Check the wiring in the tests, somehow they are returning no results
    //       even though connector runs fine (via development server).
    @Test(enabled = false)
    public void readNonPartitionedTable()
    {
        String testQuery = format("SELECT rowid, name FROM \"%s\"", NON_PARTITIONED_TABLE_NAME);
        String expResults = "SELECT * FROM VALUES('row_1', 'bob'),('row_2', 'john'),('row_3', 'tom')";
        assertHudiQuery(NON_PARTITIONED_TABLE_NAME, testQuery, expResults, false);
    }

    @Test(enabled = false)
    public void readPartitionedCowTable()
    {
        String testQuery = format("SELECT symbol, max(ts) FROM \"%s\" group by symbol HAVING symbol = 'GOOG'", PARTITIONED_COW_TABLE_NAME);
        String expResults = "SELECT * FROM VALUES('GOOG', '2018-08-31 10:59:00')";
        assertHudiQuery(PARTITIONED_COW_TABLE_NAME, testQuery, expResults, false);
    }

    @Test(enabled = false)
    public void readPartitionedMorTable()
    {
        String testQuery = format("SELECT symbol, max(ts) FROM \"%s\" group by symbol HAVING symbol = 'GOOG'", PARTITIONED_MOR_TABLE_NAME);
        String expResults = "SELECT * FROM VALUES('GOOG', '2018-08-31 10:59:00')";
        assertHudiQuery(PARTITIONED_MOR_TABLE_NAME, testQuery, expResults, false);
    }

    @Test(enabled = false)
    public void readPartitionedColumn()
    {
        String testQuery = format("SELECT dt, count(1) FROM \"%s\" group by dt", PARTITIONED_COW_TABLE_NAME);
        String expResults = "SELECT * FROM VALUES('2018/08/31', '99')";
        assertHudiQuery(PARTITIONED_COW_TABLE_NAME, testQuery, expResults, false);
    }
}

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
package io.prestosql.tests.hive.acid;

import io.prestosql.tests.hive.HiveProductTest;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class TestFullAcidTableRead
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testRead()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "full_acid_read";
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + "(a bigint)" +
                "STORED AS ORC TBLPROPERTIES ('transactional'='true')");
        try {
            assertThat(() -> query("SELECT * FROM " + tableName))
                    .failsWithMessage("Full ACID tables are not supported: default." + tableName);
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }
}

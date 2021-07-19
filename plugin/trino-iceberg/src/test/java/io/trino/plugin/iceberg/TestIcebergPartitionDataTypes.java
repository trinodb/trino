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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

public class TestIcebergPartitionDataTypes
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), new IcebergConfig().getFileFormat(), new ArrayList<>());
    }

    @BeforeClass
    public void setUp()
    {
    }

    @Test
    public void testDataTypes()
    {
        assertUpdate("CREATE TABLE ALL_DATA_TYPES(bool BOOLEAN, " +
                "int INTEGER, " +
                "big BIGINT, " +
                "rl REAL,  " +
                "dbl DOUBLE, " +
                "dec DECIMAL(5,2), " +
                "vc VARCHAR, " +
                "vb VARBINARY, " +
                "dt DATE," +
                "ts TIMESTAMP(6), " +
                "tsz TIMESTAMP(6) WITH TIME ZONE, " +
                "tm TIME(6))" +
                "WITH (PARTITIONING = ARRAY['bool', 'int', 'big', 'rl', 'dbl', 'dec', 'vc', 'vb', 'dt', 'ts', 'tm', 'tsz'])");
        assertUpdate("INSERT INTO all_data_types " +
                "SELECT true, " +
                "1, " +
                "BIGINT '2', " +
                "REAL '3.4', " +
                "DOUBLE '5.6'," +
                "DECIMAL '7.89'," +
                "'one', " +
                "from_hex('C383')," + // character Ã
                "cast('2021-04-30' as DATE)," +
                "cast('2021-04-30 00:19:31.421' as TIMESTAMP)," +
                "cast(current_timestamp as timestamp(6) with time zone)," +
                "cast('00:19:31.421' as TIME )",
                1);
        getExplainPlan("SELECT * FROM all_data_types", ExplainType.Type.IO);
        MaterializedResult baseResult = computeActual("SELECT * FROM all_data_types");
        assertEquals(baseResult.getRowCount(), 1);
        assertQuery("SELECT bool, int, big, rl, dbl, dec, vc, dt, ts, tm FROM all_data_types", "SELECT true, 1, 2, 3.4, 5.6, 7.89, 'one', " +
                "'2021-04-30', '2021-04-30 00:19:31.421', '00:19:31.421'");
        baseResult = computeActual("SELECT * FROM \"all_data_types$partitions\"");
        assertEquals(baseResult.getRowCount(), 1);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
    }
}

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
package io.trino.plugin.raptor.legacy.metadata;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.trino.plugin.raptor.legacy.DatabaseTesting.createTestingJdbi;
import static io.trino.plugin.raptor.legacy.metadata.SchemaDaoUtil.createTablesWithRetry;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestMetadataDao
{
    private MetadataDao dao;
    private Handle dummyHandle;

    @BeforeEach
    public void setup()
    {
        Jdbi dbi = createTestingJdbi();
        dummyHandle = dbi.open();
        dao = dbi.onDemand(MetadataDao.class);
        createTablesWithRetry(dbi);
    }

    @AfterEach
    public void tearDown()
    {
        dummyHandle.close();
        dummyHandle = null;
    }

    @Test
    public void testTemporalColumn()
    {
        Long columnId = 1L;
        long tableId = dao.insertTable("schema1", "table1", true, false, null, 0);
        dao.insertColumn(tableId, columnId, "col1", 1, "bigint", null, null);
        Long temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNull(temporalColumnId);

        dao.updateTemporalColumnId(tableId, columnId);
        temporalColumnId = dao.getTemporalColumnId(tableId);
        assertNotNull(temporalColumnId);
        assertEquals(temporalColumnId, columnId);

        long tableId2 = dao.insertTable("schema1", "table2", true, false, null, 0);
        Long columnId2 = dao.getTemporalColumnId(tableId2);
        assertNull(columnId2);
    }

    @Test
    public void testGetTableInformation()
    {
        Long columnId = 1L;
        long tableId = dao.insertTable("schema1", "table1", true, false, null, 0);
        dao.insertColumn(tableId, columnId, "col1", 1, "bigint", null, null);

        Table info = dao.getTableInformation(tableId);
        assertTable(info, tableId);

        info = dao.getTableInformation("schema1", "table1");
        assertTable(info, tableId);
    }

    private static void assertTable(Table info, long tableId)
    {
        assertEquals(info.getTableId(), tableId);
        assertEquals(info.getDistributionId(), Optional.empty());
        assertEquals(info.getDistributionName(), Optional.empty());
        assertEquals(info.getBucketCount(), OptionalInt.empty());
        assertEquals(info.getTemporalColumnId(), OptionalLong.empty());
    }
}

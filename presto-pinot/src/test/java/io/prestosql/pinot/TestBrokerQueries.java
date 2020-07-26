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
package io.prestosql.pinot;

import com.google.common.collect.ImmutableList;
import io.prestosql.pinot.client.PinotClient.BrokerResultRow;
import io.prestosql.pinot.client.PinotClient.ResultsIterator;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.pinot.client.PinotClient.fromResultTable;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestBrokerQueries
{
    private static final DataSchema DATA_SCHEMA;
    private static final List<Object[]> TEST_DATA;
    private static final ResultTable RESULT_TABLE;

    static
    {
        DATA_SCHEMA = new DataSchema(new String[]{"col_1", "col_2", "col_3"}, new ColumnDataType[]{STRING, LONG, STRING});
        TEST_DATA = ImmutableList.of(new Object[] {"col_1_data", 2L, "col_3_data"});
        RESULT_TABLE = new ResultTable(DATA_SCHEMA, TEST_DATA);
    }

    @Test
    public void testBrokerColumnMapping()
    {
        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .add(new PinotColumnHandle("col_2", BIGINT))
                .build();
        ResultsIterator resultIterator = fromResultTable(RESULT_TABLE, columnHandles);
        assertTrue(resultIterator.hasNext(), "resultIterator is empty");
        BrokerResultRow row = resultIterator.next();
        assertEquals(row.getField(0), "col_3_data");
        assertEquals(row.getField(1), "col_1_data");
        assertEquals(row.getField(2), 2L);
    }

    @Test
    public void testBrokerColumnMappingWithSubset()
    {
        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .build();
        ResultsIterator resultIterator = fromResultTable(RESULT_TABLE, columnHandles);
        assertTrue(resultIterator.hasNext(), "resultIterator is empty");
        BrokerResultRow row = resultIterator.next();
        assertEquals(row.getField(0), "col_3_data");
        assertEquals(row.getField(1), "col_1_data");
    }
}

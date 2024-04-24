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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotClient.BrokerResultRow;
import io.trino.plugin.pinot.client.PinotClient.ResultsIterator;
import io.trino.plugin.pinot.query.PinotQueryInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.pinot.TestPinotSplitManager.createSessionWithNumSplits;
import static io.trino.plugin.pinot.client.PinotClient.fromResultTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestBrokerQueries
        extends TestPinotQueryBase
{
    private static final BrokerResponseNative RESPONSE;
    private static final DataSchema DATA_SCHEMA;
    private static final List<Object[]> TEST_DATA;
    private static final ResultTable RESULT_TABLE;
    private static final int LIMIT_FOR_BROKER_QUERIES = 2;

    private final PinotClient testingPinotClient;

    static {
        DATA_SCHEMA = new DataSchema(new String[] {"col_1", "col_2", "col_3"}, new ColumnDataType[] {STRING, LONG, STRING});
        TEST_DATA = ImmutableList.of(new Object[] {"col_1_data", 2L, "col_3_data"});
        RESULT_TABLE = new ResultTable(DATA_SCHEMA, TEST_DATA);
        RESPONSE = new BrokerResponseNative();
        RESPONSE.setResultTable(RESULT_TABLE);
        RESPONSE.setNumServersQueried(1);
        RESPONSE.setNumServersResponded(1);
        RESPONSE.setNumDocsScanned(1);
    }

    public TestBrokerQueries()
            throws IOException
    {
        testingPinotClient = new MockPinotClient(pinotConfig, getTestingMetadata(), RESPONSE.toJsonString());
    }

    @Test
    public void testBrokerColumnMapping()
    {
        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .add(new PinotColumnHandle("col_2", BIGINT))
                .build();
        ResultsIterator resultIterator = fromResultTable(RESPONSE, columnHandles, 0);
        assertThat(resultIterator.hasNext())
                .describedAs("resultIterator is empty")
                .isTrue();
        BrokerResultRow row = resultIterator.next();
        assertThat(row.getField(0)).isEqualTo("col_3_data");
        assertThat(row.getField(1)).isEqualTo("col_1_data");
        assertThat(row.getField(2)).isEqualTo(2L);
    }

    @Test
    public void testBrokerColumnMappingWithSubset()
    {
        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .build();
        ResultsIterator resultIterator = fromResultTable(RESPONSE, columnHandles, 0);
        assertThat(resultIterator.hasNext())
                .describedAs("resultIterator is empty")
                .isTrue();
        BrokerResultRow row = resultIterator.next();
        assertThat(row.getField(0)).isEqualTo("col_3_data");
        assertThat(row.getField(1)).isEqualTo("col_1_data");
    }

    @Test
    public void testBrokerQuery()
    {
        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .add(new PinotColumnHandle("col_2", BIGINT))
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .build();
        PinotBrokerPageSource pageSource = new PinotBrokerPageSource(createSessionWithNumSplits(1, false, pinotConfig),
                new PinotQueryInfo("test_table", "SELECT col_1, col_2, col_3 FROM test_table", 0),
                columnHandles,
                testingPinotClient,
                LIMIT_FOR_BROKER_QUERIES);

        Page page = pageSource.getNextPage();
        assertThat(page.getChannelCount()).isEqualTo(columnHandles.size());
        assertThat(page.getPositionCount()).isEqualTo(RESPONSE.getResultTable().getRows().size());
        Block block = page.getBlock(0);
        String value = VARCHAR.getSlice(block, 0).toStringUtf8();
        assertThat(value).isEqualTo(getOnlyElement(RESPONSE.getResultTable().getRows())[0]);
        block = page.getBlock(1);
        assertThat(BIGINT.getLong(block, 0)).isEqualTo((long) getOnlyElement(RESPONSE.getResultTable().getRows())[1]);
        block = page.getBlock(2);
        value = VARCHAR.getSlice(block, 0).toStringUtf8();
        assertThat(value).isEqualTo(getOnlyElement(RESPONSE.getResultTable().getRows())[2]);
    }

    @Test
    public void testCountStarBrokerQuery()
    {
        PinotBrokerPageSource pageSource = new PinotBrokerPageSource(createSessionWithNumSplits(1, false, pinotConfig),
                new PinotQueryInfo("test_table", "SELECT COUNT(*) FROM test_table", 0),
                ImmutableList.of(),
                testingPinotClient,
                LIMIT_FOR_BROKER_QUERIES);
        Page page = pageSource.getNextPage();
        assertThat(page.getPositionCount()).isEqualTo(RESPONSE.getResultTable().getRows().size());
        assertThat(page.getChannelCount()).isEqualTo(0);
    }

    @Test
    public void testBrokerResponseHasTooManyRows()
            throws IOException
    {
        List<Object[]> tooManyRowsTestData = ImmutableList.<Object[]>builder()
                .add(new Object[] {"col_1_row1", 1L, "col_3_row1"})
                .add(new Object[] {"col_1_row2", 2L, "col_3_data"})
                .add(new Object[] {"col_1_row3", 3L, "col_3_data"})
                .build();
        ResultTable tooManyRowsResultTable = new ResultTable(DATA_SCHEMA, tooManyRowsTestData);
        BrokerResponseNative tooManyRowsResponse = new BrokerResponseNative();
        tooManyRowsResponse.setResultTable(tooManyRowsResultTable);
        tooManyRowsResponse.setNumServersQueried(1);
        tooManyRowsResponse.setNumServersResponded(1);
        tooManyRowsResponse.setNumDocsScanned(3);
        PinotClient testingPinotClient = new MockPinotClient(pinotConfig, getTestingMetadata(), tooManyRowsResponse.toJsonString());

        List<PinotColumnHandle> columnHandles = ImmutableList.<PinotColumnHandle>builder()
                .add(new PinotColumnHandle("col_1", VARCHAR))
                .add(new PinotColumnHandle("col_2", BIGINT))
                .add(new PinotColumnHandle("col_3", VARCHAR))
                .build();
        PinotBrokerPageSource pageSource = new PinotBrokerPageSource(createSessionWithNumSplits(1, false, pinotConfig),
                new PinotQueryInfo("test_table", "SELECT col_1, col_2, col_3 FROM test_table", 0),
                columnHandles,
                testingPinotClient,
                LIMIT_FOR_BROKER_QUERIES);
        assertThatExceptionOfType(PinotException.class)
                .isThrownBy(pageSource::getNextPage)
                .withMessage("Broker query returned '3' rows, maximum allowed is '2' rows. with query \"SELECT col_1, col_2, col_3 FROM test_table\"");
    }
}

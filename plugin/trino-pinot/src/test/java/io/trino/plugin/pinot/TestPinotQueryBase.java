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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.client.PinotClient;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestPinotQueryBase
{
    protected static PinotTableHandle realtimeOnlyTable = new PinotTableHandle("schema", "realtimeOnly");
    protected static PinotTableHandle hybridTable = new PinotTableHandle("schema", "hybrid");

    protected final PinotConfig pinotConfig = new PinotConfig().setControllerUrls("localhost:9000");

    protected final PinotClient mockClusterInfoFetcher = new MockPinotClient(pinotConfig, getTestingMetadata());
    protected final PinotMetadata pinotMetadata = new PinotMetadata(
            mockClusterInfoFetcher,
            pinotConfig,
            newCachedThreadPool(threadsNamed("mock-pinot-metadata-fetcher")));

    protected List<String> getColumnNames(String table)
    {
        return pinotMetadata.getPinotColumns(table).stream()
                .map(PinotColumnHandle::getColumnName)
                .collect(toImmutableList());
    }

    public static Map<String, Schema> getTestingMetadata()
    {
        return ImmutableMap.<String, Schema>builder()
                .put("eats_job_state", new SchemaBuilder()
                        .setSchemaName("eats_job_state")
                        .addSingleValueDimension("jobState", DataType.STRING)
                        .addMetric("regionId", DataType.LONG)
                        .build())
                .put("eats_utilization_summarized", new SchemaBuilder()
                        .setSchemaName("eats_utilization_summarized")
                        .addSingleValueDimension("activeTrips", DataType.LONG)
                        .addSingleValueDimension("numDrivers", DataType.LONG)
                        .addSingleValueDimension("region", DataType.LONG)
                        .addSingleValueDimension("rowtime", DataType.LONG)
                        .addDateTime("secondsSinceEpoch", DataType.LONG, "1:SECONDS:EPOCH", "1:SECONDS")
                        .addSingleValueDimension("utilization", DataType.LONG)
                        .addSingleValueDimension("utilizedDrivers", DataType.LONG)
                        .addSingleValueDimension("vehicleViewId", DataType.LONG)
                        .addSingleValueDimension("windowEnd", DataType.LONG)
                        .addSingleValueDimension("windowStart", DataType.LONG)
                        .build())
                .put("shopping_cart", new SchemaBuilder()
                        .setSchemaName("shopping_cart")
                        .addSingleValueDimension("shoppingCartUUID", DataType.LONG)
                        .addSingleValueDimension("$validUntil", DataType.LONG)
                        .addSingleValueDimension("$validFrom", DataType.LONG)
                        .addSingleValueDimension("jobState", DataType.LONG)
                        .addSingleValueDimension("tenancy", DataType.LONG)
                        .addSingleValueDimension("accountUUID", DataType.LONG)
                        .addSingleValueDimension("vehicleViewId", DataType.LONG)
                        .addSingleValueDimension("$partition", DataType.LONG)
                        .addSingleValueDimension("clientUUID", DataType.LONG)
                        .addSingleValueDimension("orderJobUUID", DataType.LONG)
                        .addSingleValueDimension("productTypeUUID", DataType.LONG)
                        .addSingleValueDimension("demandJobUUID", DataType.LONG)
                        .addSingleValueDimension("regionId", DataType.LONG)
                        .addSingleValueDimension("workflowUUID", DataType.LONG)
                        .addSingleValueDimension("jobType", DataType.LONG)
                        .addSingleValueDimension("kafkaOffset", DataType.LONG)
                        .addSingleValueDimension("productUUID", DataType.LONG)
                        .addSingleValueDimension("timestamp", DataType.LONG)
                        .addSingleValueDimension("flowType", DataType.LONG)
                        .addSingleValueDimension("ts", DataType.LONG)
                        .build())
                .put("test_table", new SchemaBuilder().setSchemaName("test_table")
                        .addSingleValueDimension("col_1", DataType.STRING)
                        .addSingleValueDimension("col_2", DataType.LONG)
                        .addSingleValueDimension("col_3", DataType.STRING)
                        .build())
                .put("primitive_types_table", new SchemaBuilder().setSchemaName("primitive_types_table")
                        .addSingleValueDimension("string_col", DataType.STRING)
                        .addSingleValueDimension("long_col", DataType.LONG)
                        .addSingleValueDimension("int_col", DataType.INT)
                        .addSingleValueDimension("bool_col", DataType.BOOLEAN)
                        .addSingleValueDimension("double_col", DataType.DOUBLE)
                        .addSingleValueDimension("float_col", DataType.FLOAT)
                        .addSingleValueDimension("bytes_col", DataType.BYTES)
                        .build())
                .put("quotes_in_column_names", new SchemaBuilder().setSchemaName("quotes_in_column_names")
                        .addSingleValueDimension("non_quoted", DataType.STRING)
                        .addSingleValueDimension("qu\"ot\"ed", DataType.STRING)
                        .build())
                .buildOrThrow();
    }
}

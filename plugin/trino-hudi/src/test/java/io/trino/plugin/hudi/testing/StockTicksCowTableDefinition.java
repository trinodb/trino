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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.metastore.HiveType.HIVE_DOUBLE;
import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hudi.testing.HudiRecordFactory.fieldValues;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

/**
 * Defines the stock_ticks_cow test table - a partitioned Copy-on-Write table with stock ticker data.
 */
public class StockTicksCowTableDefinition
        extends HudiTableDefinition
{
    private static final String TABLE_NAME = "stock_ticks_cow";
    private static final String RECORD_KEY_FIELD = "key";
    private static final int RECORD_COUNT = 99;

    public StockTicksCowTableDefinition()
    {
        super(
                TABLE_NAME,
                COPY_ON_WRITE,
                createRegularColumns(),
                createPartitionColumns(),
                RECORD_KEY_FIELD,
                null,  // No pre-combine field for COW
                createPartitions());
    }

    private static List<Column> createRegularColumns()
    {
        return ImmutableList.of(
                column("volume", HIVE_LONG),
                column("ts", HIVE_STRING),
                column("symbol", HIVE_STRING),
                column("year", HIVE_INT),
                column("month", HIVE_STRING),
                column("high", HIVE_DOUBLE),
                column("low", HIVE_DOUBLE),
                column("key", HIVE_STRING),
                column("date", HIVE_STRING),
                column("close", HIVE_DOUBLE),
                column("open", HIVE_DOUBLE),
                column("day", HIVE_STRING));
    }

    private static List<Column> createPartitionColumns()
    {
        return ImmutableList.of(column("dt", HIVE_STRING));
    }

    private static Map<String, String> createPartitions()
    {
        return ImmutableMap.of("dt=2018-08-31", "2018/08/31");
    }

    @Override
    protected Schema getAvroSchema()
    {
        return SchemaBuilder.record(TABLE_NAME)
                .fields()
                .requiredLong("volume")
                .requiredString("ts")
                .requiredString("symbol")
                .requiredInt("year")
                .requiredString("month")
                .requiredDouble("high")
                .requiredDouble("low")
                .requiredString("key")
                .requiredString("date")
                .requiredDouble("close")
                .requiredDouble("open")
                .requiredString("day")
                .requiredString("dt")
                .endRecord();
    }

    @Override
    public void executeCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        HudiRecordFactory recordFactory = new HudiRecordFactory(getAvroSchema(), 42L);

        List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
        String[] symbols = {"GOOG", "AAPL", "MSFT", "AMZN", "META"};

        for (int i = 0; i < RECORD_COUNT; i++) {
            String symbol = symbols[(symbols.length - 1 - (i % symbols.length)) % symbols.length];
            int minute = i % 60;
            String timestamp = String.format("2018-08-31 10:%02d:00", minute);

            records.add(recordFactory.createRecord(
                    symbol + "_2018-08-31_" + i,
                    "2018/08/31",
                    fieldValues()
                            .longField("volume", 1000L + (i * 100L))
                            .stringField("ts", timestamp)
                            .stringField("symbol", symbol)
                            .intField("year", 2018)
                            .stringField("month", "08")
                            .doubleField("high", 100.0 + (i * 0.5))
                            .doubleField("low", 90.0 + (i * 0.5))
                            .stringField("key", symbol + "_2018-08-31_" + i)
                            .stringField("date", "2018-08-31")
                            .doubleField("close", 95.0 + (i * 0.5))
                            .doubleField("open", 92.0 + (i * 0.5))
                            .stringField("day", "31")
                            .stringField("dt", "2018-08-31")
                            .build()));
        }

        String instant = client.startCommit();
        List<WriteStatus> statuses = client.insert(records, instant);
        client.commit(instant, statuses);
    }
}

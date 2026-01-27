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
import io.trino.metastore.Column;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.List;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hudi.testing.HudiRecordFactory.fieldValues;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

/**
 * Defines the hudi_non_part_cow test table - a non-partitioned Copy-on-Write table.
 */
public class HudiNonPartCowTableDefinition
        extends HudiTableDefinition
{
    private static final String TABLE_NAME = "hudi_non_part_cow";
    private static final String RECORD_KEY_FIELD = "id";

    public HudiNonPartCowTableDefinition()
    {
        super(
                TABLE_NAME,
                COPY_ON_WRITE,
                createRegularColumns(),
                RECORD_KEY_FIELD,
                null);  // No pre-combine field for COW
    }

    private static List<Column> createRegularColumns()
    {
        return ImmutableList.of(
                column("id", HIVE_LONG),
                column("name", HIVE_STRING),
                column("ts", HIVE_LONG),
                column("dt", HIVE_STRING),
                column("hh", HIVE_STRING));
    }

    @Override
    protected Schema getAvroSchema()
    {
        return SchemaBuilder.record(TABLE_NAME)
                .fields()
                .requiredLong("id")
                .requiredString("name")
                .requiredLong("ts")
                .requiredString("dt")
                .requiredString("hh")
                .endRecord();
    }

    @Override
    public void executeCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
    {
        HudiRecordFactory recordFactory = new HudiRecordFactory(getAvroSchema());

        List<HoodieRecord<HoodieAvroPayload>> records = ImmutableList.of(
                recordFactory.createRecord(
                        "id:1",
                        fieldValues()
                                .longField("id", 1L)
                                .stringField("name", "a1")
                                .longField("ts", 1000L)
                                .stringField("dt", "2021-12-09")
                                .stringField("hh", "10")
                                .build()),
                recordFactory.createRecord(
                        "id:2",
                        fieldValues()
                                .longField("id", 2L)
                                .stringField("name", "a2")
                                .longField("ts", 1000L)
                                .stringField("dt", "2021-12-09")
                                .stringField("hh", "11")
                                .build()));

        String instant = client.startCommit();
        List<WriteStatus> statuses = client.insert(records, instant);
        client.commit(instant, statuses);
    }
}

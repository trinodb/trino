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

import java.util.List;
import java.util.Map;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hudi.testing.HudiRecordFactory.fieldValues;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

/**
 * Defines the hudi_cow_pt_tbl test table - a multi-level partitioned Copy-on-Write table.
 */
public class HudiCowPtTblTableDefinition
        extends HudiTableDefinition
{
    private static final String TABLE_NAME = "hudi_cow_pt_tbl";
    private static final String RECORD_KEY_FIELD = "id";

    public HudiCowPtTblTableDefinition()
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
                column("id", HIVE_LONG),
                column("name", HIVE_STRING),
                column("ts", HIVE_LONG));
    }

    private static List<Column> createPartitionColumns()
    {
        return ImmutableList.of(
                column("dt", HIVE_STRING),
                column("hh", HIVE_STRING));
    }

    private static Map<String, String> createPartitions()
    {
        return ImmutableMap.of(
                "dt=2021-12-09/hh=10", "dt=2021-12-09/hh=10",
                "dt=2021-12-09/hh=11", "dt=2021-12-09/hh=11");
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

        // Commit 1: Insert record into partition dt=2021-12-09/hh=10
        List<HoodieRecord<HoodieAvroPayload>> firstCommitRecords = ImmutableList.of(
                recordFactory.createRecord(
                        "id:1",
                        "dt=2021-12-09/hh=10",
                        fieldValues()
                                .longField("id", 1L)
                                .stringField("name", "a1")
                                .longField("ts", 1000L)
                                .stringField("dt", "2021-12-09")
                                .stringField("hh", "10")
                                .build()));

        String instant1 = client.startCommit();
        List<WriteStatus> statuses1 = client.insert(firstCommitRecords, instant1);
        client.commit(instant1, statuses1);

        // Commit 2: Insert record into partition dt=2021-12-09/hh=11
        List<HoodieRecord<HoodieAvroPayload>> secondCommitRecords = ImmutableList.of(
                recordFactory.createRecord(
                        "id:2",
                        "dt=2021-12-09/hh=11",
                        fieldValues()
                                .longField("id", 2L)
                                .stringField("name", "a2")
                                .longField("ts", 1000L)
                                .stringField("dt", "2021-12-09")
                                .stringField("hh", "11")
                                .build()));

        String instant2 = client.startCommit();
        List<WriteStatus> statuses2 = client.insert(secondCommitRecords, instant2);
        client.commit(instant2, statuses2);
    }
}

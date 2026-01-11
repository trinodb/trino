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

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hudi.testing.HudiRecordFactory.fieldValues;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

/**
 * Defines the hudi_non_part_mor test table - a non-partitioned Merge-on-Read table.
 * <p>
 * This table demonstrates:
 * <ul>
 *   <li>Non-partitioned MOR table</li>
 *   <li>Multiple commits (bulk_insert + upsert)</li>
 *   <li>Record updates via upsert operation</li>
 * </ul>
 * <p>
 * Schema:
 * <ul>
 *   <li>id (String) - record key</li>
 *   <li>name (String) - pre-combine field</li>
 *   <li>age (Integer)</li>
 * </ul>
 * <p>
 * Data Evolution:
 * <ul>
 *   <li>Commit 1 (bulk_insert): (1, Alice, 30), (2, Bob, 25)</li>
 *   <li>Commit 2 (upsert): (1, Cathy, 30), (2, David, 25)</li>
 *   <li>Final state: (1, Cathy, 30), (2, David, 25)</li>
 * </ul>
 */
public class HudiNonPartMorTableDefinition
        extends HudiTableDefinition
{
    private static final String TABLE_NAME = "hudi_non_part_mor";
    private static final String RECORD_KEY_FIELD = "id";
    private static final String PRE_COMBINE_FIELD = "name";

    public HudiNonPartMorTableDefinition()
    {
        super(
                TABLE_NAME,
                MERGE_ON_READ,
                createRegularColumns(),
                RECORD_KEY_FIELD,
                PRE_COMBINE_FIELD);
    }

    /**
     * Defines the regular (non-Hudi metadata) columns for this table.
     */
    private static List<Column> createRegularColumns()
    {
        return ImmutableList.of(
                column("id", HIVE_STRING),
                column("name", HIVE_STRING),
                column("age", HIVE_INT));
    }

    /**
     * Creates the Avro schema for this table.
     */
    private Schema createAvroSchema()
    {
        return SchemaBuilder.record(TABLE_NAME)
                .fields()
                .requiredString("id")
                .requiredString("name")
                .requiredInt("age")
                .endRecord();
    }

    @Override
    protected Schema getAvroSchema()
    {
        return createAvroSchema();
    }

    @Override
    public void executeCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws Exception
    {
        HudiRecordFactory recordFactory = new HudiRecordFactory(getAvroSchema());

        // Commit 1: bulk_insert with Alice and Bob
        executeCommit1BulkInsert(client, recordFactory);

        // Commit 2: upsert to replace with Cathy and David
        executeCommit2Upsert(client, recordFactory);
    }

    /**
     * First commit: Bulk insert of initial records (Alice and Bob).
     */
    private void executeCommit1BulkInsert(
            HoodieJavaWriteClient<HoodieAvroPayload> client,
            HudiRecordFactory recordFactory)
    {
        List<HoodieRecord<HoodieAvroPayload>> records = ImmutableList.of(
                recordFactory.createRecord(
                        "1",
                        fieldValues()
                                .stringField("id", "1")
                                .stringField("name", "Alice")
                                .intField("age", 30)
                                .build()),
                recordFactory.createRecord(
                        "2",
                        fieldValues()
                                .stringField("id", "2")
                                .stringField("name", "Bob")
                                .intField("age", 25)
                                .build()));

        String instant = client.startCommit();
        List<WriteStatus> statuses = client.insert(records, instant);
        client.commit(instant, statuses);
    }

    /**
     * Second commit: Upsert to replace Alice with Cathy and Bob with David.
     */
    private void executeCommit2Upsert(
            HoodieJavaWriteClient<HoodieAvroPayload> client,
            HudiRecordFactory recordFactory)
    {
        List<HoodieRecord<HoodieAvroPayload>> records = ImmutableList.of(
                recordFactory.createRecord(
                        "1",
                        fieldValues()
                                .stringField("id", "1")
                                .stringField("name", "Cathy")
                                .intField("age", 30)
                                .build()),
                recordFactory.createRecord(
                        "2",
                        fieldValues()
                                .stringField("id", "2")
                                .stringField("name", "David")
                                .intField("age", 25)
                                .build()));

        String instant = client.startCommit();
        List<WriteStatus> statuses = client.upsert(records, instant);
        client.commit(instant, statuses);
    }
}

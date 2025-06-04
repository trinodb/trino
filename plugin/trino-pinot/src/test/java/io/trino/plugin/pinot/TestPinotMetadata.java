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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static io.trino.plugin.pinot.MetadataUtil.TEST_TABLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotMetadata
{
    private final PinotConfig pinotConfig = new PinotConfig().setControllerUrls(ImmutableList.of("localhost:9000"));
    private final PinotMetadata metadata = new PinotMetadata(new MockPinotClient(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor(), new PinotTypeConverter(new TestingTypeManager()));

    @Test
    public void testTables()
    {
        ConnectorSession session = TestPinotSplitManager.createSessionWithNumSplits(1, false, pinotConfig);
        List<SchemaTableName> schemaTableNames = metadata.listTables(session, Optional.empty());
        assertThat(ImmutableSet.copyOf(schemaTableNames)).isEqualTo(ImmutableSet.builder()
                .add(new SchemaTableName("default", TestPinotSplitManager.realtimeOnlyTable.tableName()))
                .add(new SchemaTableName("default", TestPinotSplitManager.hybridTable.tableName()))
                .add(new SchemaTableName("default", TEST_TABLE))
                .build());
        List<String> schemas = metadata.listSchemaNames(session);
        assertThat(ImmutableList.copyOf(schemas)).isEqualTo(ImmutableList.of("default"));
        PinotTableHandle withWeirdSchema = metadata.getTableHandle(
                session,
                new SchemaTableName("foo", TestPinotSplitManager.realtimeOnlyTable.tableName()),
                Optional.empty(),
                Optional.empty());
        assertThat(withWeirdSchema.tableName()).isEqualTo(TestPinotSplitManager.realtimeOnlyTable.tableName());
        PinotTableHandle withAnotherSchema = metadata.getTableHandle(
                session,
                new SchemaTableName(TestPinotSplitManager.realtimeOnlyTable.tableName(), TestPinotSplitManager.realtimeOnlyTable.tableName()),
                Optional.empty(),
                Optional.empty());
        assertThat(withAnotherSchema.tableName()).isEqualTo(TestPinotSplitManager.realtimeOnlyTable.tableName());
        PinotTableHandle withUppercaseTable = metadata.getTableHandle(
                session,
                new SchemaTableName("default", TEST_TABLE),
                Optional.empty(),
                Optional.empty());
        assertThat(withUppercaseTable.tableName()).isEqualTo("airlineStats");
    }
}

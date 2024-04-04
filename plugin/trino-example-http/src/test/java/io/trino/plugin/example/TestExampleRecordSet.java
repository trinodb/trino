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
package io.trino.plugin.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExampleRecordSet
{
    private ExampleHttpServer exampleHttpServer;
    private String dataUri;

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of(
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0),
                new ExampleColumnHandle("value", BIGINT, 1)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of(createUnboundedVarcharType(), BIGINT));

        recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of(
                new ExampleColumnHandle("value", BIGINT, 1),
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of(
                new ExampleColumnHandle("value", BIGINT, 1),
                new ExampleColumnHandle("value", BIGINT, 1),
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0)));
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of());
        assertThat(recordSet.getColumnTypes()).isEqualTo(ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of(
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0),
                new ExampleColumnHandle("value", BIGINT, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertThat(cursor.getType(0)).isEqualTo(createUnboundedVarcharType());
        assertThat(cursor.getType(1)).isEqualTo(BIGINT);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
            assertThat(cursor.isNull(0)).isFalse();
            assertThat(cursor.isNull(1)).isFalse();
        }
        assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .buildOrThrow());
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit(dataUri), ImmutableList.of(
                new ExampleColumnHandle("value", BIGINT, 1),
                new ExampleColumnHandle("value", BIGINT, 1),
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0)));
        RecordCursor cursor = recordSet.cursor();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertThat(cursor.getLong(0)).isEqualTo(cursor.getLong(1));
            data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
        }
        assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .buildOrThrow());
    }

    //
    // TODO: your code should also have tests for all types that you support and for the state machine of your cursor
    //

    //
    // Start http server for testing
    //

    @BeforeAll
    public void setUp()
    {
        exampleHttpServer = new ExampleHttpServer();
        dataUri = exampleHttpServer.resolve("/example-data/numbers-2.csv").toString();
    }

    @AfterAll
    public void tearDown()
    {
        if (exampleHttpServer != null) {
            exampleHttpServer.stop();
        }
    }
}

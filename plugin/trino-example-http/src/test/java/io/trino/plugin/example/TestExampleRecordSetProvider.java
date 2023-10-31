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
import io.trino.spi.connector.ConnectorTableHandle;
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
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestExampleRecordSetProvider
{
    private ExampleHttpServer exampleHttpServer;
    private String dataUri;

    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new ExampleTableHandle("schema", "table");
        ExampleRecordSetProvider recordSetProvider = new ExampleRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(ExampleTransactionHandle.INSTANCE, SESSION, new ExampleSplit(dataUri), tableHandle, ImmutableList.of(
                new ExampleColumnHandle("text", createUnboundedVarcharType(), 0),
                new ExampleColumnHandle("value", BIGINT, 1)));
        assertThat(recordSet)
                .describedAs("recordSet is null")
                .isNotNull();

        RecordCursor cursor = recordSet.cursor();
        assertThat(cursor)
                .describedAs("cursor is null")
                .isNotNull();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
        }
        assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .buildOrThrow());
    }

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

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
package io.trino.plugin.localfile;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static io.trino.plugin.localfile.LocalFileTables.HttpRequestLogTable.getSchemaTableName;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLocalFileRecordSet
{
    private static final HostAddress address = HostAddress.fromParts("localhost", 1234);

    @Test
    public void testSimpleCursor()
    {
        String location = "example-data";
        LocalFileTables localFileTables = new LocalFileTables(new LocalFileConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        LocalFileMetadata metadata = new LocalFileMetadata(localFileTables);

        assertData(localFileTables, metadata);
    }

    @Test
    public void testGzippedData()
    {
        String location = "example-gzipped-data";
        LocalFileTables localFileTables = new LocalFileTables(new LocalFileConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        LocalFileMetadata metadata = new LocalFileMetadata(localFileTables);

        assertData(localFileTables, metadata);
    }

    private static void assertData(LocalFileTables localFileTables, LocalFileMetadata metadata)
    {
        LocalFileTableHandle tableHandle = new LocalFileTableHandle(getSchemaTableName(), OptionalInt.of(0), OptionalInt.of(-1));
        List<LocalFileColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, tableHandle)
                .values().stream().map(column -> (LocalFileColumnHandle) column)
                .collect(Collectors.toList());

        LocalFileSplit split = new LocalFileSplit(address);
        RecordSet recordSet = new LocalFileRecordSet(localFileTables, split, tableHandle, columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertThat(cursor.getType(i)).isEqualTo(columnHandles.get(i).getColumnType());
        }

        // test one row
        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo(address.toString());
        assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("127.0.0.1");
        assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("POST");
        assertThat(cursor.getSlice(4).toStringUtf8()).isEqualTo("/v1/memory");
        assertThat(cursor.isNull(5)).isTrue();
        assertThat(cursor.isNull(6)).isTrue();
        assertThat(cursor.getLong(7)).isEqualTo(200);
        assertThat(cursor.getLong(8)).isEqualTo(0);
        assertThat(cursor.getLong(9)).isEqualTo(1000);
        assertThat(cursor.getLong(10)).isEqualTo(10);
        assertThat(cursor.isNull(11)).isTrue();

        assertThat(cursor.advanceNextPosition()).isTrue();
        assertThat(cursor.getSlice(0).toStringUtf8()).isEqualTo(address.toString());
        assertThat(cursor.getSlice(2).toStringUtf8()).isEqualTo("127.0.0.1");
        assertThat(cursor.getSlice(3).toStringUtf8()).isEqualTo("GET");
        assertThat(cursor.getSlice(4).toStringUtf8()).isEqualTo("/v1/service/presto/general");
        assertThat(cursor.getSlice(5).toStringUtf8()).isEqualTo("foo");
        assertThat(cursor.getSlice(6).toStringUtf8()).isEqualTo("ffffffff-ffff-ffff-ffff-ffffffffffff");
        assertThat(cursor.getLong(7)).isEqualTo(200);
        assertThat(cursor.getLong(8)).isEqualTo(0);
        assertThat(cursor.getLong(9)).isEqualTo(37);
        assertThat(cursor.getLong(10)).isEqualTo(1094);
        assertThat(cursor.getSlice(11).toStringUtf8()).isEqualTo("a7229d56-5cbd-4e23-81ff-312ba6be0f12");
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}

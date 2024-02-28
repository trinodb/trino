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
package io.trino.orc.stream;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.checkpoint.StreamCheckpoint;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.Stream;
import io.trino.orc.metadata.Stream.StreamKind;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestValueStream<T, C extends StreamCheckpoint, W extends ValueOutputStream<C>, R extends ValueInputStream<C>>
{
    static final int COMPRESSION_BLOCK_SIZE = 256 * 1024;
    static final OrcDataSourceId ORC_DATA_SOURCE_ID = new OrcDataSourceId("test");

    protected void testWriteValue(List<List<T>> groups)
            throws IOException
    {
        W outputStream = createValueOutputStream();
        for (int i = 0; i < 3; i++) {
            outputStream.reset();
            long retainedBytes = 0;
            for (List<T> group : groups) {
                outputStream.recordCheckpoint();
                group.forEach(value -> writeValue(outputStream, value));

                assertThat(outputStream.getRetainedBytes() >= retainedBytes).isTrue();
                retainedBytes = outputStream.getRetainedBytes();
            }
            outputStream.close();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
            StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(new OrcColumnId(33));
            streamDataOutput.writeData(sliceOutput);
            Stream stream = streamDataOutput.getStream();
            assertThat(stream.getStreamKind()).isEqualTo(StreamKind.DATA);
            assertThat(stream.getColumnId()).isEqualTo(new OrcColumnId(33));
            assertThat(stream.getLength()).isEqualTo(sliceOutput.size());

            List<C> checkpoints = outputStream.getCheckpoints();
            assertThat(checkpoints.size()).isEqualTo(groups.size());

            R valueStream = createValueStream(sliceOutput.slice());
            for (List<T> group : groups) {
                int index = 0;
                for (T expectedValue : group) {
                    index++;
                    T actualValue = readValue(valueStream);
                    if (!actualValue.equals(expectedValue)) {
                        assertThat(actualValue)
                                .describedAs("index=" + index)
                                .isEqualTo(expectedValue);
                    }
                }
            }
            for (int groupIndex = groups.size() - 1; groupIndex >= 0; groupIndex--) {
                valueStream.seekToCheckpoint(checkpoints.get(groupIndex));
                for (T expectedValue : groups.get(groupIndex)) {
                    T actualValue = readValue(valueStream);
                    if (!actualValue.equals(expectedValue)) {
                        assertThat(actualValue).isEqualTo(expectedValue);
                    }
                }
            }
        }
    }

    protected abstract W createValueOutputStream();

    protected abstract void writeValue(W outputStream, T value);

    protected abstract R createValueStream(Slice slice)
            throws OrcCorruptionException;

    protected abstract T readValue(R valueStream)
            throws IOException;
}

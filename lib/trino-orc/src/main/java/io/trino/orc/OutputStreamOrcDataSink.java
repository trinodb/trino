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
package io.trino.orc;

import io.airlift.slice.OutputStreamSliceOutput;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.stream.OrcDataOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

public class OutputStreamOrcDataSink
        implements OrcDataSink
{
    private static final int INSTANCE_SIZE = instanceSize(OutputStreamOrcDataSink.class);

    private final OutputStreamSliceOutput output;
    private final AggregatedMemoryContext memoryContext;

    public static OutputStreamOrcDataSink create(TrinoOutputFile outputFile)
            throws IOException
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        return new OutputStreamOrcDataSink(outputFile.create(memoryContext), memoryContext);
    }

    // Do not use this method, it is here only for io.trino.plugin.raptor.legacy.storage.OrcFileWriter.createOrcDataSink
    // and it should be removed in the future
    @Deprecated
    public static OutputStreamOrcDataSink create(OutputStream outputStream)
            throws IOException
    {
        return new OutputStreamOrcDataSink(outputStream, newSimpleAggregatedMemoryContext());
    }

    private OutputStreamOrcDataSink(OutputStream outputStream, AggregatedMemoryContext memoryContext)
    {
        this.output = new OutputStreamSliceOutput(requireNonNull(outputStream, "outputStream is null"));
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public long size()
    {
        return output.longSize();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + output.getRetainedSize() + memoryContext.getBytes();
    }

    @Override
    public void write(List<OrcDataOutput> outputData)
    {
        outputData.forEach(data -> data.writeData(output));
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }
}

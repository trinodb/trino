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
package io.trino.plugin.deltalake.kernel;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.kernel.data.AbstractTrinoColumnVectorWrapper;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

public class KernelDeltaLakePageSource
        implements ConnectorPageSource
{
    private final TrinoFileSystem trinoFileSystem;
    private final TypeManager typeManager;
    private final KernelDeltaLakeSplit split;
    private final KernelDeltaLakeTableHandle tableHandle;

    // working state
    private CloseableIterator<FilteredColumnarBatch> dataBatchIterator;

    public KernelDeltaLakePageSource(
            TrinoFileSystem trinoFileSystem,
            TypeManager typeManager,
            KernelDeltaLakeSplit split,
            KernelDeltaLakeTableHandle tableHandle)
    {
        this.trinoFileSystem = trinoFileSystem;
        this.typeManager = typeManager;
        this.split = split;
        this.tableHandle = tableHandle;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        initBatchIteratorIfNotDone();
        return !dataBatchIterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        initBatchIteratorIfNotDone();
        if (isFinished()) {
            return null;
        }

        FilteredColumnarBatch nextBatch = dataBatchIterator.next();
        Page page = convertDeltaToTrino(nextBatch.getData());

        return nextBatch.getSelectionVector()
                .map(selectionVector -> withSelectionPositions(page, selectionVector))
                .orElse(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private void initBatchIteratorIfNotDone()
    {
        if (dataBatchIterator == null) {
            Engine engine = KernelClient.createEngine(trinoFileSystem, typeManager);
            Row scanState = split.getScanState(engine);
            Row scanFile = split.getScanFile(engine);
            FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile);
            StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);
            try {
                CloseableIterator<ColumnarBatch> physicalDataIter =
                        engine.getParquetHandler().readParquetFiles(
                                singletonCloseableIterator(fileStatus),
                                physicalReadSchema,
                                Optional.empty());
                dataBatchIterator =
                        Scan.transformPhysicalData(
                                engine,
                                scanState,
                                scanFile,
                                physicalDataIter);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static Page convertDeltaToTrino(ColumnarBatch columnarBatch)
    {
        List<Block> blocks = new ArrayList<>();
        for (int columnIdx = 0; columnIdx < columnarBatch.getSchema().length(); columnIdx++) {
            ColumnVector deltaVector = columnarBatch.getColumnVector(columnIdx);
            if (deltaVector instanceof AbstractTrinoColumnVectorWrapper trinoDeltaVector) {
                blocks.add(trinoDeltaVector.getTrinoBlock());
            }
            else {
                throw new UnsupportedOperationException("Encountered vectors that are not Trino based.");
            }
        }
        return new Page(columnarBatch.getSize(), blocks.toArray(new Block[0]));
    }

    private static Page withSelectionPositions(
            Page page,
            ColumnVector selectionColumnVector)
    {
        int positionCount = page.getPositionCount();
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (selectionColumnVector.getBoolean(position)) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount == positionCount) {
            return page;
        }
        return page.getPositions(retained, 0, retainedCount);
    }
}

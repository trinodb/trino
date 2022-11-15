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
package io.trino.plugin.iceberg;

import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.SortingFileWriter;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergSortingFileWriter
        implements IcebergFileWriter
{
    private final IcebergFileWriter outputWriter;
    private final SortingFileWriter sortingFileWriter;

    public IcebergSortingFileWriter(
            TrinoFileSystem fileSystem,
            Path tempFilePrefix,
            IcebergFileWriter outputWriter,
            DataSize maxMemory,
            int maxOpenTempFiles,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter,
            TypeOperators typeOperators)
    {
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.sortingFileWriter = new SortingFileWriter(
                fileSystem,
                tempFilePrefix,
                outputWriter,
                maxMemory,
                maxOpenTempFiles,
                types,
                sortFields,
                sortOrders,
                pageSorter,
                typeOperators,
                OrcFileWriterFactory::createOrcDataSink);
    }

    @Override
    public Metrics getMetrics()
    {
        return outputWriter.getMetrics();
    }

    @Override
    public long getWrittenBytes()
    {
        return sortingFileWriter.getWrittenBytes();
    }

    @Override
    public long getMemoryUsage()
    {
        return sortingFileWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        sortingFileWriter.appendRows(dataPage);
    }

    @Override
    public Closeable commit()
    {
        return sortingFileWriter.commit();
    }

    @Override
    public void rollback()
    {
        sortingFileWriter.rollback();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return sortingFileWriter.getValidationCpuNanos();
    }

    @Override
    public Optional<Runnable> getVerificationTask()
    {
        return sortingFileWriter.getVerificationTask();
    }
}

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
package io.trino.plugin.iceberg.system.partitions;

import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.Type;
import org.apache.iceberg.FileScanTask;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public record PartitionsTableSplit(
        List<FileScanTaskData> fileScanTasks,
        String schemaJson,
        Map<Integer, String> partitionSpecsByIdJson,
        Optional<Type> partitionColumnType,
        Optional<Type> dataColumnType,
        Map<String, String> fileIoProperties)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(PartitionsTableSplit.class);

    @Override
    public long getRetainedSizeInBytes()
    {
        // partitionColumnType and dataColumnType not accounted for as Type instances are cached and shared
        return INSTANCE_SIZE
                + estimatedSizeOf(fileScanTasks, FileScanTaskData::getRetainedSizeInBytes)
                + estimatedSizeOf(schemaJson)
                + estimatedSizeOf(partitionSpecsByIdJson, SizeOf::sizeOf, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(fileIoProperties, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }

    public record FileScanTaskData(
            String filePath,
            long length,
            int partitionSpecId,
            String partitionDataJson)
    {
        private static final int INSTANCE_SIZE = instanceSize(FileScanTaskData.class);

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(filePath)
                    + SizeOf.sizeOf(length)
                    + SizeOf.sizeOf(partitionSpecId)
                    + estimatedSizeOf(partitionDataJson);
        }

        public static FileScanTaskData from(FileScanTask fileScanTask)
        {
            return new FileScanTaskData(
                    fileScanTask.file().location(),
                    fileScanTask.file().fileSizeInBytes(),
                    fileScanTask.spec().specId(),
                    fileScanTask.file().partition().toString());
        }
    }
}
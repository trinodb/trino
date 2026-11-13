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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import io.trino.spi.TrinoException;

import static io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT;

/**
 * Maps a Lakehouse partition to the Trino metastore model.
 */
public final class PartitionMapper
{
    private PartitionMapper() {}

    public static io.trino.metastore.Partition toTrino(Partition partition, String databaseName, String tableName)
    {
        String qualifiedName = databaseName + "." + tableName + " partition " + partition.getValuesList();

        if (!partition.hasStorageDescriptor()) {
            throw new TrinoException(
                    HIVE_UNSUPPORTED_FORMAT, "Partition has no storage descriptor: " + qualifiedName);
        }
        StorageDescriptor descriptor = partition.getStorageDescriptor();

        return new io.trino.metastore.Partition(
                databaseName,
                tableName,
                partition.getValuesList(),
                StorageMapper.toTrino(descriptor, qualifiedName),
                StorageMapper.toTrinoColumns(descriptor.getColumnsList(), qualifiedName),
                partition.getParametersMap());
    }
}

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
package io.trino.plugin.iceberg.delete;

import io.airlift.units.DataSize;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Estimates the memory held by an opened delete file from the small file threshold of its format.
 * The Parquet and ORC readers read a file in full when opening it if it is not larger than their threshold.
 * The Avro reader always streams the file.
 */
public final class SmallFileAwareDeletePageSourceProvider
        implements DeletePageSourceProvider
{
    private final DeletePageSourceProvider delegate;
    private final long parquetSmallFileThresholdBytes;
    private final long orcTinyStripeThresholdBytes;

    public SmallFileAwareDeletePageSourceProvider(DeletePageSourceProvider delegate, DataSize parquetSmallFileThreshold, DataSize orcTinyStripeThreshold)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.parquetSmallFileThresholdBytes = parquetSmallFileThreshold.toBytes();
        this.orcTinyStripeThresholdBytes = orcTinyStripeThreshold.toBytes();
    }

    @Override
    public ConnectorPageSource openDeletes(DeleteFile delete, List<IcebergColumnHandle> deleteColumns, TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        return delegate.openDeletes(delete, deleteColumns, tupleDomain);
    }

    @Override
    public long estimateOpenedBytes(DeleteFile delete)
    {
        long fileSize = delete.fileSizeInBytes();
        long smallFileThreshold = switch (IcebergFileFormat.fromIceberg(delete.format())) {
            case PARQUET -> parquetSmallFileThresholdBytes;
            case ORC -> orcTinyStripeThresholdBytes;
            case AVRO -> 0;
        };
        if (fileSize <= smallFileThreshold) {
            return fileSize;
        }
        return 0;
    }
}

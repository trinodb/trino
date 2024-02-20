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
package io.trino.orc.metadata;

import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;
import io.trino.orc.metadata.statistics.BloomFilter;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfPossible;
import static java.util.Objects.requireNonNull;

public class ExceptionWrappingMetadataReader
        implements MetadataReader
{
    private final OrcDataSourceId orcDataSourceId;
    private final MetadataReader delegate;

    public ExceptionWrappingMetadataReader(OrcDataSourceId orcDataSourceId, MetadataReader delegate)
    {
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(!(delegate instanceof ExceptionWrappingMetadataReader), "ExceptionWrappingMetadataReader cannot wrap a ExceptionWrappingMetadataReader");
    }

    @Override
    public PostScript readPostScript(InputStream inputStream)
            throws OrcCorruptionException
    {
        try {
            return delegate.readPostScript(inputStream);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid postscript");
        }
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws OrcCorruptionException
    {
        try {
            return delegate.readMetadata(hiveWriterVersion, inputStream);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid file metadata");
        }
    }

    @Override
    public Footer readFooter(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws OrcCorruptionException
    {
        try {
            return delegate.readFooter(hiveWriterVersion, inputStream);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid file footer");
        }
    }

    @Override
    public StripeFooter readStripeFooter(ColumnMetadata<OrcType> types, InputStream inputStream, ZoneId legacyFileTimeZone)
            throws IOException
    {
        try {
            return delegate.readStripeFooter(types, inputStream, legacyFileTimeZone);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid stripe footer");
        }
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws OrcCorruptionException
    {
        try {
            return delegate.readRowIndexes(hiveWriterVersion, inputStream);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid stripe row index");
        }
    }

    @Override
    public List<BloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws OrcCorruptionException
    {
        try {
            return delegate.readBloomFilterIndexes(inputStream);
        }
        catch (IOException | RuntimeException e) {
            throw propagate(e, "Invalid bloom filter");
        }
    }

    private OrcCorruptionException propagate(Throwable throwable, String message)
    {
        propagateIfPossible(throwable, TrinoException.class);
        return new OrcCorruptionException(throwable, orcDataSourceId, "%s", message);
    }
}

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
package io.trino.plugin.hive.esri;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.esri.EsriDeserializer;
import io.trino.hive.formats.esri.EsriReader;
import io.trino.hive.formats.line.Column;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.hive.formats.HiveClassNames.ESRI_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.ESRI_SERDE_CLASS;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.util.Objects.requireNonNull;

public class EsriPageSourceFactory
        implements HivePageSourceFactory
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public EsriPageSourceFactory(TrinoFileSystemFactory trinoFileSystemFactory)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
    }

    @Override
    public Optional<ConnectorPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            Schema schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!ESRI_SERDE_CLASS.equals(schema.serializationLibraryName())
                || !ESRI_INPUT_FORMAT_CLASS.equals(schema.serdeProperties().get(FILE_INPUT_FORMAT))) {
            return Optional.empty();
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported for Esri files");

        // Skip empty inputs
        if (length == 0) {
            return Optional.of(new EmptyPageSource());
        }

        if (start != 0) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Split start must be 0 for Esri files");
        }

        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
        TrinoInputFile inputFile = trinoFileSystem.newInputFile(path);

        // TODO: optimization for small files that should just be read into memory. Consider it later.
        try {
            InputStream inputStream = inputFile.newStream();

            List<Column> decoderColumns = columns.stream()
                    .map(hc -> new Column(hc.getName(), hc.getType(), hc.getBaseHiveColumnIndex()))
                    .collect(toImmutableList());

            EsriDeserializer esriDeserializer = new EsriDeserializer(decoderColumns);
            EsriReader esriReader = new EsriReader(inputStream, esriDeserializer);
            EsriPageSource pageSource = new EsriPageSource(esriReader, columns, path);

            return Optional.of(pageSource);
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
    }
}

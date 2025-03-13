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
package io.trino.plugin.hive.ion;

import com.amazon.ion.IonReader;
import com.amazon.ion.system.IonReaderBuilder;
import com.google.common.io.CountingInputStream;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hive.formats.compression.Codec;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.ion.IonDecoder;
import io.trino.hive.formats.ion.IonDecoderConfig;
import io.trino.hive.formats.ion.IonDecoderFactory;
import io.trino.hive.formats.line.Column;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.Schema;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hive.formats.HiveClassNames.ION_SERDE_CLASS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.util.HiveUtil.splitError;

public class IonPageSourceFactory
        implements HivePageSourceFactory
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final boolean nativeTrinoEnabled;

    @Inject
    public IonPageSourceFactory(TrinoFileSystemFactory trinoFileSystemFactory, HiveConfig hiveConfig)
    {
        this.trinoFileSystemFactory = trinoFileSystemFactory;
        this.nativeTrinoEnabled = hiveConfig.getIonNativeTrinoEnabled();
    }

    @Override
    public Optional<ConnectorPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long lastModifiedTime,
            Schema schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!nativeTrinoEnabled
                || !ION_SERDE_CLASS.equals(schema.serializationLibraryName())) {
            return Optional.empty();
        }

        if (IonSerDeProperties.hasUnsupportedProperty(schema.serdeProperties())) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Error creating Ion Input, Table contains unsupported SerDe properties for native Ion");
        }

        checkArgument(acidInfo.isEmpty(), "Acid is not supported for Ion files");

        // Skip empty inputs
        if (length == 0) {
            return Optional.of(new EmptyPageSource());
        }

        if (start != 0) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Split start must be 0 for Ion files");
        }

        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
        TrinoInputFile inputFile = trinoFileSystem.newInputFile(path, estimatedFileSize);

        // todo: optimization for small files that should just be read into memory
        try {
            Optional<Codec> codec = CompressionKind.forFile(inputFile.location().fileName())
                    .map(CompressionKind::createCodec);

            CountingInputStream countingInputStream = new CountingInputStream(inputFile.newStream());
            InputStream inputStream;
            if (codec.isPresent()) {
                inputStream = codec.get().createStreamDecompressor(countingInputStream);
            }
            else {
                inputStream = countingInputStream;
            }

            IonDecoderConfig decoderConfig = IonSerDeProperties.decoderConfigFor(schema.serdeProperties());

            return Optional.of(projectColumnDereferences(columns, baseColumns -> {
                PageBuilder pageBuilder = new PageBuilder(baseColumns.stream()
                        .map(HiveColumnHandle::getType)
                        .toList());
                List<Column> decoderColumns = baseColumns.stream()
                        .map(hc -> new Column(hc.getName(), hc.getType(), hc.getBaseHiveColumnIndex()))
                        .toList();

                IonDecoder decoder = IonDecoderFactory.buildDecoder(decoderColumns, decoderConfig, pageBuilder);
                IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
                return new IonPageSource(ionReader, countingInputStream::getCount, decoder, pageBuilder);
            }));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
    }
}

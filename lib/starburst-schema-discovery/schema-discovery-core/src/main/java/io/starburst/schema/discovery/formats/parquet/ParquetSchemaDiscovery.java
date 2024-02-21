/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.parquet;

import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.SchemaDiscovery;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.spi.TrinoException;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static java.util.Objects.requireNonNull;

public class ParquetSchemaDiscovery
        implements SchemaDiscovery
{
    private final ParquetDataSourceFactory dataSourceFactory;

    private static final FormatGuess FORMAT_MATCH = () -> FormatGuess.Confidence.HIGH;

    public ParquetSchemaDiscovery(ParquetDataSourceFactory dataSourceFactory)
    {
        this.dataSourceFactory = requireNonNull(dataSourceFactory, "dataSourceFactory is null");
    }

    @Override
    public DiscoveredColumns discoverColumns(DiscoveryInput stream, Map<String, String> options)
    {
        try {
            ParquetDataSource dataSource = createParquetDataSource(stream);
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
            MessageType schema = metadata.getFileMetaData().getSchema();
            ImmutableList<Column> columns = schema.asGroupType()
                    .getFields()
                    .stream()
                    .map(this::map)
                    .flatMap(Optional::stream)
                    .collect(toImmutableList());
            return new DiscoveredColumns(columns, ImmutableList.of());
        }
        catch (IOException e) {
            throw new TrinoException(IO, e);
        }
    }

    @Override
    public Optional<FormatGuess> checkFormatMatch(DiscoveryInput stream)
    {
        byte[] bytes = new byte[4];
        try {
            if ((stream.asInputStream().read(bytes) == 4) && (bytes[0] == 'P') && (bytes[1] == 'A') && (bytes[2] == 'R') && (bytes[3] == '1')) {
                return Optional.of(FORMAT_MATCH);
            }
        }
        catch (EOFException ignore) {
            // ignore and return empty
        }
        catch (IOException e) {
            throw new TrinoException(IO, e);
        }
        return Optional.empty();
    }

    private ParquetDataSource createParquetDataSource(DiscoveryInput stream)
            throws IOException
    {
        return dataSourceFactory.build(stream.asTrinoFile());
    }

    private Optional<Column> map(org.apache.parquet.schema.Type type)
    {
        return HiveTypes.toColumn(type.getName(), Converter.fromParquetType(type));
    }
}

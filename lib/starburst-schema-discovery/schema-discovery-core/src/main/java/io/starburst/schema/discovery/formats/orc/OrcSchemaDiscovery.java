/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.orc;

import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.SchemaDiscovery;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.orc.AbstractOrcDataSource;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.TrinoException;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static java.util.Objects.requireNonNull;

public class OrcSchemaDiscovery
        implements SchemaDiscovery
{
    private final OrcDataSourceFactory dataSourceFactory;

    private static final FormatGuess FORMAT_MATCH = () -> FormatGuess.Confidence.HIGH;

    public OrcSchemaDiscovery(OrcDataSourceFactory dataSourceFactory)
    {
        this.dataSourceFactory = requireNonNull(dataSourceFactory, "dataSourceFactory is null");
    }

    @Override
    public DiscoveredColumns discoverColumns(DiscoveryInput stream, Map<String, String> options)
    {
        OrcReader orcReader = createOrcReader(stream);
        List<OrcType> collect = orcReader.getFooter().getTypes().stream().collect(toImmutableList());
        ImmutableList<Column> columns = orcReader.getRootColumn()
                .getNestedColumns()
                .stream()
                .map(orcColumn -> map(orcColumn, collect))
                .flatMap(Optional::stream)
                .collect(toImmutableList());
        return new DiscoveredColumns(columns, ImmutableList.of());
    }

    @Override
    public Optional<FormatGuess> checkFormatMatch(DiscoveryInput stream)
    {
        byte[] bytes = new byte[3];
        try {
            if ((stream.asInputStream().read(bytes) == 3) && (bytes[0] == 'O') && (bytes[1] == 'R') && (bytes[2] == 'C')) {
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

    private Optional<Column> map(OrcColumn orcColumn, List<OrcType> typesLookup)
    {
        return HiveTypes.toColumn(orcColumn.getColumnName(), Converter.fromOrcType(orcColumn, typesLookup));
    }

    private OrcReader createOrcReader(DiscoveryInput stream)
    {
        try {
            OrcDataSourceId id = new OrcDataSourceId(UUID.randomUUID().toString());
            OrcReaderOptions options = new OrcReaderOptions();
            AbstractOrcDataSource orcDataSource = dataSourceFactory.build(id, stream.length(), options, stream.asTrinoFile());
            return OrcReader.createOrcReader(orcDataSource, options).orElseThrow(() -> new IllegalArgumentException("Failed to create ORC reader"));
        }
        catch (IOException e) {
            throw new TrinoException(IO, e);
        }
    }
}

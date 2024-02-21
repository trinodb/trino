/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.csv;

import io.starburst.schema.discovery.SchemaDiscovery;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.options.OptionsMap;

import java.util.Map;
import java.util.Optional;

public class CsvSchemaDiscovery
        implements SchemaDiscovery
{
    public static final CsvSchemaDiscovery INSTANCE = new CsvSchemaDiscovery();

    private CsvSchemaDiscovery() {}

    @Override
    public DiscoveredColumns discoverColumns(DiscoveryInput stream, Map<String, String> options)
    {
        CsvOptions csvOptions = new CsvOptions(new OptionsMap(options));
        try (CsvStreamSampler csvProcessor = new CsvStreamSampler(csvOptions, stream)) {
            CsvInferSchema csvInferSchema = new CsvInferSchema(csvProcessor, csvOptions);
            return csvInferSchema.infer(csvProcessor.fieldsAreQuoted());
        }
    }

    @Override
    public Optional<FormatGuess> checkFormatMatch(DiscoveryInput stream)
    {
        return CheckCsvFormat.checkFormatMatch(stream);
    }
}

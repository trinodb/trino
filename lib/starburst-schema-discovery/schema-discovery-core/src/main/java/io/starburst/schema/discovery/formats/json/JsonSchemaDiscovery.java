/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.SchemaDiscovery;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.internal.TextFileLines;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.options.OptionsMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.internal.TextFileLines.TestLinesMode.ALL_MATCH;
import static io.starburst.schema.discovery.internal.TextFileLines.TestLinesMode.ANY_MATCH;

public class JsonSchemaDiscovery
        implements SchemaDiscovery
{
    private static final ObjectMapper JSON_PARSER = new ObjectMapperProvider().get().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final FormatGuess FORMAT_MATCH_HIGH = () -> FormatGuess.Confidence.HIGH;
    private static final FormatGuess FORMAT_MATCH_LOW = () -> FormatGuess.Confidence.HIGH;

    public static final JsonSchemaDiscovery INSTANCE = new JsonSchemaDiscovery();

    private JsonSchemaDiscovery() {}

    @Override
    public DiscoveredColumns discoverColumns(DiscoveryInput stream, Map<String, String> optionsMap)
    {
        OptionsMap options = new OptionsMap(optionsMap);
        JsonStreamSampler streamSampler = new JsonStreamSampler(JSON_PARSER, options, stream.asInputStream());
        JsonInferSchema inferSchema = new JsonInferSchema(options, streamSampler);
        List<Column> columns = inferSchema.infer();
        List<String> examples = streamSampler.firstNode()
                .map(this::nodeToExamples)
                .orElseGet(ImmutableList::of);

        List<Column> columnsWithExamples = IntStream.range(0, columns.size())
                .mapToObj(i -> new Column(
                        columns.get(i).name(),
                        columns.get(i).type(),
                        i < examples.size() && !isNullOrEmpty(examples.get(i)) ? Optional.of(examples.get(i)) : Optional.empty()))
                .collect(toImmutableList());
        return new DiscoveredColumns(columnsWithExamples, ImmutableList.of());
    }

    private List<String> nodeToExamples(JsonNode node)
    {
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder();
        for (JsonNode n : node) {
            builder.add(n.asText());
        }
        return builder.build();
    }

    @Override
    public Optional<FormatGuess> checkFormatMatch(DiscoveryInput stream)
    {
        Optional<byte[]> testBytes = TextFileLines.getTestBytes(stream);
        return regularJsonFileCheck(testBytes)
                .or(largeJsonFileCheck(testBytes));
    }

    private static Optional<FormatGuess> regularJsonFileCheck(Optional<byte[]> testBytes)
    {
        return testBytes.filter(bytes -> TextFileLines.testLines(bytes, ALL_MATCH, s -> s.startsWith("{") && s.endsWith("}")))
                .map(__ -> FORMAT_MATCH_HIGH);
    }

    private static Supplier<Optional<? extends FormatGuess>> largeJsonFileCheck(Optional<byte[]> testBytes)
    {
        // in case of big jsons, which overflow our test buffer, not whole object might fit, so there is single line with no json end
        return () -> testBytes.filter(bytes -> TextFileLines.testLine(bytes, ANY_MATCH, s -> s.startsWith("{")))
                .map(__ -> FORMAT_MATCH_LOW);
    }
}

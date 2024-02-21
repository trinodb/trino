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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.starburst.schema.discovery.internal.LineStreamSampler;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Stream;

class JsonStreamSampler
{
    private final BufferedReader reader;
    private final ObjectMapper objectMapper;
    private final Optional<JsonNode> firstNode;
    private final LineStreamSampler<JsonNode> lineStreamSampler;
    private boolean firstNodeIsActive;

    JsonStreamSampler(ObjectMapper objectMapper, OptionsMap options, InputStream in)
    {
        reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.objectMapper = objectMapper;
        firstNode = nextLine();
        firstNodeIsActive = true;   // must be after firstNode assignment
        GeneralOptions generalOptions = new GeneralOptions(options);
        lineStreamSampler = new LineStreamSampler<>(generalOptions.maxSampleLines(), generalOptions.sampleLinesModulo(), this::nextLine);
    }

    Stream<JsonNode> stream()
    {
        return lineStreamSampler.stream();
    }

    Optional<JsonNode> firstNode()
    {
        return firstNode;
    }

    private Optional<JsonNode> nextLine()
    {
        if (firstNodeIsActive) {
            firstNodeIsActive = false;
            return firstNode;
        }

        try {
            String line = reader.readLine();
            if (line != null) {
                return Optional.of(objectMapper.readTree(line));
            }
        }
        catch (IOException e) {
            // TODO
        }
        return Optional.empty();
    }
}

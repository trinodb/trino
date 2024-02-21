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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

class JsonController
{
    private final List<JsonNode> jsonNodes;
    private final List<String> columnNames;

    JsonController(JsonStreamSampler sampler)
    {
        jsonNodes = sampler.stream().toList();
        Set<String> columnNames = new LinkedHashSet<>();
        jsonNodes.forEach(node -> columnNames.addAll(ImmutableSet.copyOf(node.fieldNames())));
        this.columnNames = ImmutableList.copyOf(columnNames);
    }

    Stream<JsonNode> stream()
    {
        return jsonNodes.stream();
    }

    List<String> columnNames()
    {
        return columnNames;
    }
}

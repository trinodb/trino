/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.JdbcSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OracleSplit
        extends JdbcSplit
{
    private final Optional<List<String>> partitionNames;

    @JsonCreator
    public OracleSplit(
            @JsonProperty("partitionNames") Optional<List<String>> partitionNames,
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate)
    {
        super(additionalPredicate);

        requireNonNull(partitionNames, "partitionNames is null");
        partitionNames.ifPresent(names -> checkArgument(!names.isEmpty(), "partitionNames cannot be empty if present"));
        this.partitionNames = partitionNames.map(ImmutableList::copyOf);
    }

    @JsonProperty
    public Optional<List<String>> getPartitionNames()
    {
        return partitionNames;
    }
}

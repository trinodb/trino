/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.tests.odbc.protocol;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.client.QueryResults;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ExtendedQueryResults
{
    private final QueryResults results;
    private final Map<String, String> addedPreparedStatements;
    private final Set<String> deallocatedPreparedStatements;

    public ExtendedQueryResults(QueryResults results, Map<String, String> addedPreparedStatements, Set<String> deallocatedPreparedStatements)
    {
        this.results = requireNonNull(results, "results is null");
        this.addedPreparedStatements = ImmutableMap.copyOf(requireNonNull(addedPreparedStatements, "addedPreparedStatements is null"));
        this.deallocatedPreparedStatements = ImmutableSet.copyOf(requireNonNull(deallocatedPreparedStatements, "deallocatedPreparedStatements is null"));
    }

    @JsonUnwrapped
    @JsonProperty
    public QueryResults getResults()
    {
        return results;
    }

    @JsonProperty
    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @JsonProperty
    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }
}

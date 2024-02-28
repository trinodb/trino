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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QuerySubmission
{
    private final String query;
    private final Map<String, String> preparedStatements;

    @JsonCreator
    public QuerySubmission(
            @JsonProperty("query") String query,
            @JsonProperty("preparedStatements") Map<String, String> preparedStatements)
    {
        this.query = requireNonNull(query, "query is null");
        this.preparedStatements = ImmutableMap.copyOf(requireNonNull(preparedStatements, "preparedStatements is null"));
    }

    public String getQuery()
    {
        return query;
    }

    public Map<String, String> getPreparedStatements()
    {
        return preparedStatements;
    }
}

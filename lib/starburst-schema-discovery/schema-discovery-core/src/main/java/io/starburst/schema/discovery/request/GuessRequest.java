/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.request;

import com.google.common.collect.ImmutableMap;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public record GuessRequest(URI uri, Map<String, String> options)
{
    public GuessRequest
    {
        requireNonNull(uri, "uri cannot be null");
        options = ImmutableMap.copyOf(options);
    }
}

/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import io.starburst.schema.discovery.models.LowerCaseString;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record Column(LowerCaseString name, HiveType type, Optional<String> sampleValue)
{
    public Column
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
        requireNonNull(type, "sampleValue is null");
    }

    public Column(LowerCaseString name, HiveType type)
    {
        this(name, type, Optional.empty());
    }

    public Column withSampleValue(String sampleValue)
    {
        return new Column(name, type, Optional.of(sampleValue));
    }

    public Column withoutSampleValue()
    {
        return new Column(name, type, Optional.empty());
    }
}

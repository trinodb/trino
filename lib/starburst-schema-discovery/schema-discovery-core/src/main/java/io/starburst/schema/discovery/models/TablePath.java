/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public sealed interface TablePath
        extends Comparable<TablePath>
        permits ArbitraryPath, SlashEndedPath
{
    @JsonValue
    String path();

    boolean isEmpty();

    @Override
    default int compareTo(TablePath o)
    {
        return this.path().compareTo(o.path());
    }

    @JsonCreator
    static TablePath asTablePath(String string)
    {
        return string.endsWith("/") ? SlashEndedPath.ensureEndsWithSlash(string) : new ArbitraryPath(string);
    }
}

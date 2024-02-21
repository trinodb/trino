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

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public record LowerCaseString(String string)
{
    public LowerCaseString
    {
        requireNonNull(string, "string is null");
        string = string.toLowerCase(Locale.ENGLISH);
    }

    @JsonCreator
    public static LowerCaseString toLowerCase(String name)
    {
        return new LowerCaseString(name.toLowerCase(Locale.ENGLISH));
    }

    @JsonValue
    @Override
    public String string()
    {
        return string;
    }

    @Override
    public String toString()
    {
        return this.string;
    }
}

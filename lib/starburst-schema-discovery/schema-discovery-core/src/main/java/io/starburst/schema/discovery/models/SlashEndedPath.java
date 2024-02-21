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
import io.trino.filesystem.Location;

import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record SlashEndedPath(String path)
        implements Comparable<SlashEndedPath>
{
    public static final SlashEndedPath SINGLE_SLASH_EMPTY = new SlashEndedPath("/");

    public SlashEndedPath
    {
        requireNonNull(path, "path is null");
        checkArgument(path.endsWith("/"), "path needs to end with /");
    }

    @JsonValue
    @Override
    public String path()
    {
        return path;
    }

    @JsonCreator
    public static SlashEndedPath ensureEndsWithSlash(String string)
    {
        return new SlashEndedPath(string.endsWith("/") ? string : string + "/");
    }

    public static SlashEndedPath ensureEndsWithSlash(URI uri)
    {
        return ensureEndsWithSlash(uri.toString());
    }

    public static SlashEndedPath ensureEndsWithSlash(Location path)
    {
        return ensureEndsWithSlash(path.toString());
    }

    @Override
    public String toString()
    {
        return this.path;
    }

    public boolean isEmpty()
    {
        return SINGLE_SLASH_EMPTY.equals(this);
    }

    @Override
    public int compareTo(SlashEndedPath o)
    {
        return this.path.compareTo(o.path());
    }
}

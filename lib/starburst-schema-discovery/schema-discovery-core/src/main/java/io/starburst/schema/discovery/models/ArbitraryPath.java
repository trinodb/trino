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

public record ArbitraryPath(String path)
        implements TablePath
{
    public static final ArbitraryPath ARBITRARY_PATH_EMPTY = new ArbitraryPath("");

    @Override
    public boolean isEmpty()
    {
        return ARBITRARY_PATH_EMPTY.equals(this);
    }

    @Override
    public String toString()
    {
        return this.path;
    }
}

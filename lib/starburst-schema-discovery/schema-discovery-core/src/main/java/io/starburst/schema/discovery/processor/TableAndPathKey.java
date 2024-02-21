/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import io.starburst.schema.discovery.TableChanges.TableName;
import io.trino.filesystem.Location;

import java.util.Objects;

// we need table name as a key but we also want to know the path it came from
// this Key only matches on table name but also contains the path for future reference
public record TableAndPathKey(TableName tableName, Location path)
{
    @Override
    // NOTE: only tableName is considered
    public boolean equals(Object o)
    {
        return (this == o) || ((o instanceof TableAndPathKey that) && tableName.equals(that.tableName));
    }

    @Override
    // NOTE: only tableName is considered
    public int hashCode()
    {
        return Objects.hash(tableName);
    }
}

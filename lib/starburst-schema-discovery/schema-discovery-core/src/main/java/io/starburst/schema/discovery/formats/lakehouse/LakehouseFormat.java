/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.lakehouse;

import io.starburst.schema.discovery.models.TableFormat;
import io.trino.filesystem.Location;

import static java.util.Objects.requireNonNull;

public record LakehouseFormat(TableFormat format, Location path)
{
    public LakehouseFormat
    {
        requireNonNull(format, "format is null");
        requireNonNull(path, "path is null");
    }
}

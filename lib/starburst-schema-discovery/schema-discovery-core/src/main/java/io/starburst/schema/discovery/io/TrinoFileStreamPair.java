/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.io;

import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import static java.util.Objects.requireNonNull;

public record TrinoFileStreamPair(TrinoInputFile inputFile, TrinoInputStream inputStream)
{
    public TrinoFileStreamPair
    {
        requireNonNull(inputFile, "inputFile is null");
        requireNonNull(inputStream, "inputStream is null");
    }
}

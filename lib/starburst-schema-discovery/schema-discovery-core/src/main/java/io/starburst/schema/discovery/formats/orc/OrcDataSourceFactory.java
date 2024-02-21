/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.orc;

import io.trino.filesystem.TrinoInputFile;
import io.trino.orc.AbstractOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReaderOptions;

import java.io.IOException;

public interface OrcDataSourceFactory
{
    AbstractOrcDataSource build(OrcDataSourceId id, long size, OrcReaderOptions options, TrinoInputFile inputFile)
            throws IOException;
}

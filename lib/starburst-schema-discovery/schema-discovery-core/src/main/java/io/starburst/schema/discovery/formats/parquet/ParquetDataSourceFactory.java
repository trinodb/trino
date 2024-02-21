/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.parquet;

import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetDataSource;

import java.io.IOException;

public interface ParquetDataSourceFactory
{
    ParquetDataSource build(TrinoInputFile inputFile)
            throws IOException;
}

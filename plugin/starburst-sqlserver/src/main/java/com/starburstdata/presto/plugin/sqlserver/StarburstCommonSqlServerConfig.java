/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class StarburstCommonSqlServerConfig
{
    private boolean bulkCopyForWrite;

    public boolean isBulkCopyForWrite()
    {
        return bulkCopyForWrite;
    }

    @Config("sqlserver.bulk-copy-for-write.enabled")
    @ConfigDescription("Use SQL Server Bulk Copy API for writes")
    public StarburstCommonSqlServerConfig setBulkCopyForWrite(boolean bulkCopyForWrite)
    {
        this.bulkCopyForWrite = bulkCopyForWrite;
        return this;
    }
}

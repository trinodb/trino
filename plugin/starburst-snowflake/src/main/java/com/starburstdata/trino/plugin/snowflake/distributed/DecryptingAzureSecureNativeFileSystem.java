/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */

package com.starburstdata.trino.plugin.snowflake.distributed;

import org.apache.hadoop.fs.azure.DecryptingAzureNativeFileSystemStore;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;

public class DecryptingAzureSecureNativeFileSystem
        extends NativeAzureFileSystem
{
    public DecryptingAzureSecureNativeFileSystem()
    {
        super(new DecryptingAzureNativeFileSystemStore());
    }

    @Override
    public String getScheme()
    {
        return "wasbs";
    }
}

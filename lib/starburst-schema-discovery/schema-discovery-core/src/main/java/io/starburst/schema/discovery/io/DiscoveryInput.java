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

import java.io.InputStream;

public interface DiscoveryInput
        extends AutoCloseable
{
    long length();

    DiscoveryInput rewind();

    InputStream asInputStream();

    TrinoInputFile asTrinoFile();
}

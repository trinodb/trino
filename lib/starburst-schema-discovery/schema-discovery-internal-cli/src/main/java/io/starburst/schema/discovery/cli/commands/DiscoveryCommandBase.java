/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.cli.commands;

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.net.URI;

abstract sealed class DiscoveryCommandBase
        permits SchemaDiscoveryCommand, ShallowDiscoveryCommand
{
    protected static DiscoveryTrinoFileSystem getFileSystem(URI uri)
    {
        if ((uri.getScheme() != null) && uri.getScheme().equals("s3")) {
            return new DiscoveryTrinoFileSystem(buildS3FileSystem());
        }
        throw new RuntimeException("Not supported yet");
    }

    protected static TrinoFileSystem buildS3FileSystem()
    {
        S3FileSystemFactory s3FileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        // credentials are taken from default chain, which usually is ~/.aws/credentials [default] profile
                        .setRegion("us-east-1")
                        .setStreamingPartSize(DataSize.valueOf("5.5MB")));

        return s3FileSystemFactory.create(ConnectorIdentity.ofUser("local-discovery"));
    }
}

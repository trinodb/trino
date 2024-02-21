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

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static io.starburst.schema.discovery.io.LocationUtils.parentOf;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static org.assertj.core.api.Assertions.assertThat;

class LocationUtilsTest
{
    @Test
    void testDirectoryOrFileName()
    {
        assertThat(directoryOrFileName(Location.of("/Users/foo/bar/baz")))
                .isEqualTo("baz");

        assertThat(directoryOrFileName(Location.of("/Users/foo/bar/baz/")))
                .isEqualTo("baz");

        assertThat(directoryOrFileName(Location.of("/Users/foo/bar/baz.json")))
                .isEqualTo("baz.json");

        assertThat(directoryOrFileName(Location.of("s3://bucket/folder/file1")))
                .isEqualTo("file1");

        assertThat(directoryOrFileName(Location.of("s3://bucket/folder/nested/")))
                .isEqualTo("nested");

        assertThat(directoryOrFileName(Location.of("s3://bucket/folder/nested/file1.json")))
                .isEqualTo("file1.json");
    }

    @Test
    void testUriFromLocation()
    {
        assertThat(uriFromLocation(Location.of("s3://bucket/folder/nested/")))
                .isEqualTo(URI.create("s3://bucket/folder/nested/"));

        assertThat(uriFromLocation(Location.of("local:///bucket/folder/nested/")))
                .isEqualTo(URI.create("local:///bucket/folder/nested/"));
    }

    @Test
    void testParentOf()
    {
        assertThat(parentOf(Location.of("s3://bucket/folder/nested/file1.json")))
                .isEqualTo(Location.of("s3://bucket/folder/nested"));

        assertThat(parentOf(Location.of("s3://bucket/folder/nested/")))
                .isEqualTo(Location.of("s3://bucket/folder/nested"));

        assertThat(parentOf(Location.of("s3://bucket/folder/nested")))
                .isEqualTo(Location.of("s3://bucket/folder"));
    }
}

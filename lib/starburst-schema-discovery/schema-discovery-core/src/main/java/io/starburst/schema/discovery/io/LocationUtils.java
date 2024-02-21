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

import java.net.URI;

public class LocationUtils
{
    private LocationUtils() {}

    // Location does not return .fileName() when it ends with / (is a directory)
    public static String directoryOrFileName(Location location)
    {
        String fullLocation = location.toString();
        if (fullLocation.endsWith("/")) {
            return fullLocation.substring(
                    fullLocation.substring(0, fullLocation.length() - 1).lastIndexOf("/") + 1,
                    fullLocation.length() - 1);
        }
        return location.fileName();
    }

    public static URI uriFromLocation(Location location)
    {
        return URI.create(location.toString());
    }

    public static Location parentOf(Location location)
    {
        return location.path().endsWith("/") ?
                Location.of(location.toString().substring(0, location.toString().length() - 1)) :
                location.parentDirectory();
    }
}

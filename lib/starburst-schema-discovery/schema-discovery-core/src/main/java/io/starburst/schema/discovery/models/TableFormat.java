/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;

import java.util.Set;

public enum TableFormat
{
    JSON(true, FormatExtensionMatch.matchingExtension(".json")),
    CSV(true, FormatExtensionMatch.matchingExtension(".csv")),
    ORC(true, FormatExtensionMatch.matchingExtension(".orc")),
    PARQUET(true, FormatExtensionMatch.matchingExtension(".parquet")),
    ICEBERG(false, FormatExtensionMatch.notDeterminedByExtension()),
    DELTA_LAKE(false, FormatExtensionMatch.notDeterminedByExtension()),
    ERROR(true, FormatExtensionMatch.notDeterminedByExtension());

    public boolean requiresColumnDefinitions()
    {
        return requiresColumnDefinitions;
    }

    public boolean isFileMatching(Location filePath)
    {
        if (!extensionMatch.canBeDeterminedByFileExtension()) {
            return false;
        }
        String fileName = filePath.fileName();
        int position = fileName.lastIndexOf(46);
        if (position < 0) {
            return false;
        }
        String extension = fileName.substring(position);
        return extensionMatch.matchingFileExtensions().contains(extension);
    }

    private final boolean requiresColumnDefinitions;
    private final FormatExtensionMatch extensionMatch;

    TableFormat(boolean requiresColumnDefinitions, FormatExtensionMatch extensionMatch)
    {
        this.requiresColumnDefinitions = requiresColumnDefinitions;
        this.extensionMatch = extensionMatch;
    }

    private record FormatExtensionMatch(boolean canBeDeterminedByFileExtension, Set<String> matchingFileExtensions)
    {
        private static FormatExtensionMatch matchingExtension(String matchingExtension)
        {
            return new FormatExtensionMatch(true, ImmutableSet.of(matchingExtension));
        }

        private static FormatExtensionMatch notDeterminedByExtension()
        {
            return new FormatExtensionMatch(false, ImmutableSet.of());
        }
    }
}

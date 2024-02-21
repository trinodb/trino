/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor.filetracker;

import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.trino.filesystem.Location;

public class FileTrackerFactory
{
    private FileTrackerFactory() {}

    public static FileTracker createFileTracker(OptionsMap optionsMap, Location root)
    {
        GeneralOptions generalOptions = new GeneralOptions(optionsMap);
        return generalOptions.discoveryMode() == DiscoveryMode.RECURSIVE_DIRECTORIES ?
                new RecursiveDirectoriesFileTracker(root, generalOptions) :
                new PartitionedDiscoveryFileTracker(root, generalOptions);
    }
}

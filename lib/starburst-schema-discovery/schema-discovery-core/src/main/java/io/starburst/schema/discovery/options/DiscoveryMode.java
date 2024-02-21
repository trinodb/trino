/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.options;

public enum DiscoveryMode
{
    /**
     * In this mode discovery expects to start in rootUri,
     * which direct children are directories with tables, which can
     * contain hierarchy of directories inside them, which will be transparent to discovery
     * (they won't be perceived as partitions etc.)
     * Similar to hive.recursive-directories in trino
     */
    RECURSIVE_DIRECTORIES,

    /**
     * Default
     * In this mode discovery crawls for data files, and builds structure from bottom to top,
     * where data files are expected to be in either flat table (table directory -> data files),
     * or hierarchy where each folder in table is a partition (key=value)
     * Any directory above table will be discovered as schema
     */
    NORMAL
}

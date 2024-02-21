/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.request;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record GenerateOptions(String defaultSchemaName, int bucketQty, boolean includePartitions, Optional<String> catalogName)
{
    public GenerateOptions
    {
        requireNonNull(defaultSchemaName, "defaultSchemaName is null");
        requireNonNull(catalogName, "catalogName is null");
    }
}

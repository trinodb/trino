/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import io.starburst.schema.discovery.internal.FormatGuess;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.models.DiscoveredColumns;

import java.util.Map;
import java.util.Optional;

@FunctionalInterface
public interface SchemaDiscovery
{
    DiscoveredColumns discoverColumns(DiscoveryInput in, Map<String, String> options);

    default Optional<FormatGuess> checkFormatMatch(DiscoveryInput in)
    {
        return Optional.empty();
    }
}

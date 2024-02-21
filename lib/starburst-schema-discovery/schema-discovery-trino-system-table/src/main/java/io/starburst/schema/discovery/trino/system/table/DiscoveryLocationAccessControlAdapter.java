/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.trino.system.table;

import io.trino.spi.security.ConnectorIdentity;

@FunctionalInterface
public interface DiscoveryLocationAccessControlAdapter
{
    DiscoveryLocationAccessControlAdapter ALLOW_ALL = (identity, location) -> {};

    void checkCanUseLocation(ConnectorIdentity identity, String location);
}

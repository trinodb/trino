/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.jdbc;

import com.google.common.annotations.VisibleForTesting;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

import java.util.Objects;
import java.util.Optional;

import static com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeJdbcSessionProperties.getWarehouse;
import static java.util.Objects.requireNonNull;

class WarehouseAwareIdentityCacheMapping
        implements IdentityCacheMapping
{
    private final IdentityCacheMapping delegate;

    WarehouseAwareIdentityCacheMapping(IdentityCacheMapping delegate)
    {
        this.delegate = requireNonNull(delegate);
    }

    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        return new Key(
                delegate.getRemoteUserCacheKey(session),
                getWarehouse(session));
    }

    @VisibleForTesting
    static final class Key
            extends IdentityCacheKey
    {
        private final IdentityCacheKey superKey;
        private final Optional<String> warehouse;

        Key(IdentityCacheKey superKey, Optional<String> warehouse)
        {
            this.superKey = requireNonNull(superKey);
            this.warehouse = requireNonNull(warehouse);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(superKey, warehouse);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key that = (Key) o;
            return superKey.equals(that.superKey) && warehouse.equals(that.warehouse);
        }
    }
}

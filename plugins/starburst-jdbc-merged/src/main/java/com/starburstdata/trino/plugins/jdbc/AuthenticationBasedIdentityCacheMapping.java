/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.jdbc;

import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.spi.connector.ConnectorSession;

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AuthenticationBasedIdentityCacheMapping
        implements IdentityCacheMapping
{
    @Override
    public IdentityCacheKey getRemoteUserCacheKey(ConnectorSession session)
    {
        return new Key(session.getIdentity().getUser(), session.getIdentity().getPrincipal().map(Principal::getName));
    }

    private static class Key
            extends IdentityCacheKey
    {
        private final String user;
        private final Optional<String> principalName;

        public Key(String user, Optional<String> principalName)
        {
            this.user = requireNonNull(user, "user is null");
            this.principalName = requireNonNull(principalName, "principalName is null");
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
            Key key = (Key) o;
            return Objects.equals(user, key.user) && Objects.equals(principalName, key.principalName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, principalName);
        }
    }
}

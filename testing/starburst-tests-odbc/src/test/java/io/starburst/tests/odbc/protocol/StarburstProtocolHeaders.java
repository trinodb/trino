/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.tests.odbc.protocol;

import io.trino.client.ProtocolDetectionException;
import io.trino.client.ProtocolHeaders;

import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StarburstProtocolHeaders
{
    private final ProtocolHeaders delegate;

    public StarburstProtocolHeaders(ProtocolHeaders delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public String requestPreparedStatementInBody()
    {
        return format("X-%s-Prepared-Statement-In-Body", delegate.getProtocolName());
    }

    public String requestPreparedStatement()
    {
        return delegate.requestPreparedStatement();
    }

    public String responseAddedPrepare()
    {
        return delegate.responseAddedPrepare();
    }

    public String responseDeallocatedPrepare()
    {
        return delegate.responseDeallocatedPrepare();
    }

    public static StarburstProtocolHeaders detectProtocol(Optional<String> alternateHeaderName, Set<String> headers)
            throws ProtocolDetectionException
    {
        return new StarburstProtocolHeaders(ProtocolHeaders.detectProtocol(alternateHeaderName, headers));
    }
}

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

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.trino.client.ProtocolDetectionException;
import io.trino.client.QueryResults;
import io.trino.server.ProtocolConfig;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.starburst.tests.odbc.protocol.StarburstProtocolHeaders.detectProtocol;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;

@Provider
public class StatementResponseFilter
        implements ContainerResponseFilter
{
    private final Optional<String> alternateHeaderName;

    @Inject
    public StatementResponseFilter(ProtocolConfig protocolConfig)
    {
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response)
    {
        StarburstProtocolHeaders headers = detectProtocolHeaders(request.getHeaders());

        if (request.getHeaderString(headers.requestPreparedStatementInBody()) == null ||
                response.getStatus() != OK.getStatusCode() ||
                !response.getMediaType().equals(MediaType.APPLICATION_JSON_TYPE) ||
                !(response.getEntity() instanceof QueryResults)) {
            return;
        }

        Map<String, String> added = new HashMap<>();
        List<String> addedHeaders = response.getStringHeaders().remove(headers.responseAddedPrepare());
        if (addedHeaders != null) {
            for (String header : addedHeaders) {
                List<String> split = Splitter.on("=").splitToList(header);
                added.put(decode(split.get(0), UTF_8), decode(split.get(1), UTF_8));
            }
        }

        Set<String> deallocated = new HashSet<>();
        List<String> deallocatedHeaders = response.getStringHeaders().remove(headers.responseDeallocatedPrepare());
        if (deallocatedHeaders != null) {
            for (String header : deallocatedHeaders) {
                deallocated.add(decode(header, UTF_8));
            }
        }

        QueryResults queryResults = (QueryResults) response.getEntity();
        response.setEntity(new ExtendedQueryResults(queryResults, added, deallocated));
    }

    private StarburstProtocolHeaders detectProtocolHeaders(MultivaluedMap<String, String> headers)
    {
        try {
            return detectProtocol(alternateHeaderName, headers.keySet());
        }
        catch (ProtocolDetectionException e) {
            throw badRequest(e.getMessage());
        }
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(message, Response
                .status(Response.Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }
}

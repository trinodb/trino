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

import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.client.ProtocolDetectionException;
import io.trino.server.ProtocolConfig;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.starburst.tests.odbc.protocol.StarburstProtocolHeaders.detectProtocol;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PreparedStatementRequestFilter
        extends StatementRequestFilter
{
    private final Optional<String> alternateHeaderName;

    @Inject
    public PreparedStatementRequestFilter(ProtocolConfig protocolConfig)
    {
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @Override
    protected ContainerRequestFilter create()
    {
        return new RequestFilter();
    }

    private class RequestFilter
            implements ContainerRequestFilter
    {
        private static final JsonCodec<QuerySubmission> QUERY_SUBMISSION_CODEC = jsonCodec(QuerySubmission.class);

        @Override
        public void filter(ContainerRequestContext request)
                throws IOException
        {
            StarburstProtocolHeaders protocolHeaders = detectProtocolHeaders(request.getHeaders());

            if (request.getHeaderString(protocolHeaders.requestPreparedStatementInBody()) == null ||
                    !request.hasEntity()) {
                return;
            }

            QuerySubmission submission = QUERY_SUBMISSION_CODEC.fromJson(toByteArray(request.getEntityStream()));

            request.getHeaders().putSingle(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString());
            request.setEntityStream(new ByteArrayInputStream(submission.getQuery().getBytes(UTF_8)));

            // We need to add headers according to detected protocol so they will be recognized internally
            submission.getPreparedStatements().forEach((name, value) -> request.getHeaders()
                    .add(protocolHeaders.requestPreparedStatement(), encode(name, UTF_8) + "=" + encode(value, UTF_8)));
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
}

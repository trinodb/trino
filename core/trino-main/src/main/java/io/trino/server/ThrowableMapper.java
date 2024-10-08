/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.InternalServerErrorException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.ext.ExceptionMapper;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;

public class ThrowableMapper
        implements ExceptionMapper<Throwable>
{
    private static final Logger log = Logger.get(ThrowableMapper.class);

    private final boolean includeExceptionInResponse;

    @Context
    private HttpServletRequest request;

    @Inject
    public ThrowableMapper(ServerConfig config)
    {
        includeExceptionInResponse = config.isIncludeExceptionInResponse();
    }

    @Override
    public Response toResponse(Throwable throwable)
    {
        // In HTTP/2 status consists of the code alone, without the reason phrase which exists only in the HTTP/1.
        // Airlift enabled RESPONSE_SET_STATUS_OVER_SEND_ERROR which means that for 4xx and 5xx status codes,
        // HttpServletResponse.setStatus will be called instead of HttpServletResponse.sendError.
        //
        // HttpServletResponse.sendError is problematic, as usually it resets entity, response headers and provide error page
        // which is then rendered by the container implementation (e.g. Jetty). The generated errors depend on the implementation
        // of the container and may not be consistent across different versions.
        //
        // Another problem with HttpServletResponse.sendError is that if the ServletFilter is used, it can't access
        // ServletRequest/ServletResponse objects as they can be recycled by Jetty after sendError was called.
        //
        // With RESPONSE_SET_STATUS_OVER_SEND_ERROR enabled, the jax-rs application controls the process of returning errors
        // and ServletFilters can access the request/response objects.
        return switch (throwable) {
            case ForbiddenException forbiddenException -> plainTextError(Response.Status.FORBIDDEN)
                    .entity("Error 403 Forbidden: " + forbiddenException.getMessage())
                    .build();
            case ServiceUnavailableException serviceUnavailableException -> plainTextError(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Error 503 Service Unavailable: " + serviceUnavailableException.getMessage())
                    .build();
            case NotFoundException notFoundException -> plainTextError(Response.Status.NOT_FOUND)
                    .entity("Error 404 Not Found: " + notFoundException.getMessage())
                    .build();
            case BadRequestException badRequestException -> plainTextError(Response.Status.BAD_REQUEST)
                    .entity("Error 400 Bad Request: " + badRequestException.getMessage())
                    .build();
            case InternalServerErrorException internalServerErrorException -> plainTextError(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error 500 Internal Server Error: " + internalServerErrorException.getMessage())
                    .build();
            case ServerErrorException serverErrorException -> plainTextError(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error 500 Internal Server Error: " + serverErrorException.getMessage())
                    .build();
            case GoneException goneException -> plainTextError(Response.Status.GONE)
                    .entity("Error 410 Gone: " + goneException.getMessage())
                    .build();
            case WebApplicationException webApplicationException -> webApplicationException.getResponse();
            default -> {
                log.warn(throwable, "Request failed for %s", request.getRequestURI());

                ResponseBuilder responseBuilder = plainTextError(Response.Status.INTERNAL_SERVER_ERROR);
                if (includeExceptionInResponse) {
                    responseBuilder.entity(Throwables.getStackTraceAsString(throwable));
                }
                else {
                    responseBuilder.entity("Exception processing request");
                }
                yield responseBuilder.build();
            }
        };
    }

    private static ResponseBuilder plainTextError(Response.Status status)
    {
        return Response.status(status).header(CONTENT_TYPE, TEXT_PLAIN);
    }
}

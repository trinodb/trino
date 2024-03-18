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
package io.trino.plugin.varada.util;

import com.google.common.base.Throwables;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.spi.TrinoException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;

import java.util.Optional;

import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;

public class TrinoExceptionMapper
        implements ExceptionMapper<Throwable>
{
    public static final int VARADA_ERROR_CODE_OFFSET = 0xdb0000;
    private static final int MAX_DEPTH = 10;

    @Override
    public Response toResponse(Throwable throwable)
    {
        String message = null;
        Optional<TrinoException> trinoExceptionOpt = getTrinoException(throwable);
        if (trinoExceptionOpt.isPresent()) {
            Optional<VaradaErrorCode> varadaErrorCodeOpt = getFromCode(trinoExceptionOpt.get().getErrorCode().getCode());
            if (varadaErrorCodeOpt.isPresent()) {
                message = String.format("%s (code %d)", trinoExceptionOpt.get().getMessage(), trinoExceptionOpt.get().getErrorCode().getCode() - VARADA_ERROR_CODE_OFFSET);
            }
        }

        if (message == null) {
            message = Throwables.getStackTraceAsString(throwable);
        }
        Response.ResponseBuilder responseBuilder = Response.serverError()
                .header(CONTENT_TYPE, TEXT_PLAIN);
        responseBuilder.entity(message);
        return responseBuilder.build();
    }

    private Optional<TrinoException> getTrinoException(Throwable throwable)
    {
        int depth = 0;
        Throwable t = throwable;
        while (t != null && depth < MAX_DEPTH) {
            if (t instanceof TrinoException) {
                return Optional.of((TrinoException) t);
            }
            t = t.getCause();
            depth++;
        }
        return Optional.empty();
    }

    public Optional<VaradaErrorCode> getFromCode(int code)
    {
        for (VaradaErrorCode varadaErrorCode : VaradaErrorCode.values()) {
            if (varadaErrorCode.toErrorCode().getCode() == code) {
                return Optional.of(varadaErrorCode);
            }
        }
        return Optional.empty();
    }
}

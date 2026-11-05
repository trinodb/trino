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
package io.trino.cli;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.trino.client.ErrorLocation;
import io.trino.client.QueryError;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;

/**
 * Formats errors as a single line of JSON for programs that read the standard error of the CLI.
 */
final class AgentErrors
{
    private static final JsonFactory JSON_FACTORY = JsonPrinter.jsonFactory();

    private AgentErrors() {}

    static String exception(Throwable throwable)
    {
        String message = throwable.getMessage();
        if (message == null) {
            message = throwable.getClass().getName();
        }
        return message(message);
    }

    static String message(String message)
    {
        return render(null, message, null);
    }

    static String queryMessage(String queryId, String message)
    {
        return render(queryId, message, null);
    }

    static String queryFailure(String queryId, QueryError error)
    {
        return render(queryId, error.getMessage(), error);
    }

    private static String render(String queryId, String message, QueryError error)
    {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = JSON_FACTORY.createGenerator(writer)) {
            generator.writeStartObject();
            generator.writeStringField("type", "error");
            if (queryId != null) {
                generator.writeStringField("query_id", queryId);
            }
            if (error != null) {
                if (error.getErrorName() != null) {
                    generator.writeStringField("error_name", error.getErrorName());
                }
                if (error.getErrorType() != null) {
                    generator.writeStringField("error_type", error.getErrorType());
                }
                generator.writeNumberField("error_code", error.getErrorCode());
                if (error.getSqlState() != null) {
                    generator.writeStringField("sql_state", error.getSqlState());
                }
            }
            generator.writeStringField("message", message);
            if (error != null && error.getErrorLocation() != null) {
                ErrorLocation location = error.getErrorLocation();
                generator.writeObjectFieldStart("location");
                generator.writeNumberField("line", location.getLineNumber());
                generator.writeNumberField("column", location.getColumnNumber());
                generator.writeEndObject();
            }
            generator.writeEndObject();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return writer.toString();
    }
}

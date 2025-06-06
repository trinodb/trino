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
package io.trino.plugin.prometheus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class LoggedRequest
{
    @JsonProperty("request_method")
    public String method;

    @JsonProperty("request_uri")
    public String uri;

    @JsonProperty("status")
    public int status;

    private String extractGetParameter(String parameterName)
    {
        int paramIndex = uri.indexOf(parameterName + "=");
        if (paramIndex < 0) {
            return null;
        }
        int endIndex = uri.indexOf('&', paramIndex);
        if (endIndex < 0) {
            endIndex = uri.length();
        }
        return URLDecoder.decode(
                uri.substring(paramIndex + parameterName.length() + 1, endIndex),
                StandardCharsets.UTF_8);
    }

    public String extractPromQL()
    {
        return extractGetParameter("query");
    }

    public String extractTime()
    {
        return extractGetParameter("time");
    }

    public String extractStart()
    {
        return extractGetParameter("start");
    }

    public String extractEnd()
    {
        return extractGetParameter("end");
    }

    public String extractStep()
    {
        return extractGetParameter("step");
    }

    public String extractLimit()
    {
        return extractGetParameter("limit");
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s -> %s", method, uri, status);
    }
}

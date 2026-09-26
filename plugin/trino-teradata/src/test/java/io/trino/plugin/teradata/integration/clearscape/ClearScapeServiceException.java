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
package io.trino.plugin.teradata.integration.clearscape;

import static java.util.Objects.requireNonNull;

public class ClearScapeServiceException
        extends RuntimeException
{
    public ClearScapeServiceException(int statusCode, String body)
    {
        super(buildMessage(statusCode, requireNonNull(body, "body should not be null")));
    }

    private static String buildMessage(int statusCode, String body)
    {
        if (statusCode >= 400 && statusCode <= 499) {
            return "Client error - " + statusCode + body;
        }
        if (statusCode >= 500 && statusCode <= 599) {
            return "Server error - " + statusCode + body;
        }
        return "Unexpected error - " + statusCode + body;
    }
}

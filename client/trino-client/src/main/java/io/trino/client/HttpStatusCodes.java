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
package io.trino.client;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

public final class HttpStatusCodes
{
    private HttpStatusCodes() {}

    public static boolean shouldRetry(int statusCode)
    {
        switch (statusCode) {
            case HTTP_BAD_GATEWAY:
            case HTTP_UNAVAILABLE:
            case HTTP_GATEWAY_TIMEOUT:
                return true;
            default:
                return false;
        }
    }
}

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
package io.trino.plugin.fission;

import java.io.IOException;

public class FissionFunctionConfigProvider
{
    private static final String FISSION_FUNCTION_BASE_URL_ENV_VAR = "FISSION_FUNCTION_BASE_URL";

    private static String fissionFunctionBaseUrl;

    private FissionFunctionConfigProvider() {}

    /**
     * Fetches env var 'FISSION_FUNCTION_BASE_URL' and returns it as a string, if it is not set will throw an error
     *
     * @throws IOException if FISSION_FUNCTION_BASE_URL env var is not set
     * @return fissionFunctionBaseUrl - string representation of the url for fission endpoints
     */
    public static String getFissionFunctionBaseURL() throws IOException
    {
        if (fissionFunctionBaseUrl == null) {
            fissionFunctionBaseUrl = System.getenv(FISSION_FUNCTION_BASE_URL_ENV_VAR);
            if (fissionFunctionBaseUrl == null || fissionFunctionBaseUrl.isEmpty()) {
                String errorMesseage = String.format("%s is not set.", FISSION_FUNCTION_BASE_URL_ENV_VAR);
                throw new IOException(errorMesseage);
            }
        }
        return fissionFunctionBaseUrl;
    }
}

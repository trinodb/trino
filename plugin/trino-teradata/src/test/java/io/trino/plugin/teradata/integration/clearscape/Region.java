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

import java.util.Arrays;

public enum Region {
    US_CENTRAL("us-central"),
    US_EAST("us-east"),
    US_WEST("us-west"),
    SOUTHAMERICA_EAST("southamerica-east"),
    EUROPE_WEST("europe-west"),
    ASIA_SOUTH("asia-south"),
    ASIA_NORTHEAST("asia-northeast"),
    ASIA_SOUTHEAST("asia-southeast"),
    AUSTRALIA_SOUTHEAST("australia-southeast");

    private final String code;

    Region(String code)
    {
        this.code = code;
    }

    public static boolean isValid(String value)
    {
        if (value == null) {
            return false;
        }
        return Arrays.stream(Region.values())
                .anyMatch(r -> r.code.equalsIgnoreCase(value));
    }
}

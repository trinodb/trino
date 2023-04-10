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

package io.trino.server.security.jwt;

import com.google.common.base.Strings;

import java.util.Arrays;

public enum TokenTypeEnum {
    JWT("jwt"),
    AZURE_AD("azure_ad");

    private final String type;

    TokenTypeEnum(String type)
    {
        this.type = type;
    }

    public String getType()
    {
        return this.type;
    }

    public static TokenTypeEnum fromTokenType(String tokenType)
    {
        return Strings.isNullOrEmpty(tokenType) ? JWT : Arrays.stream(values())
                .filter(tokenTypeEnum -> tokenTypeEnum.type.equalsIgnoreCase(tokenType))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unknown token type"));
    }
}

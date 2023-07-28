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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class QueryDataFormats
{
    private static final Splitter SPLITTER = Splitter.on(",");
    private static final Joiner JOINER = Joiner.on(",");

    private QueryDataFormats() {}

    public static String toHeaderValue(Set<String> supportedQueryDataFormats)
    {
        return JOINER.join(supportedQueryDataFormats);
    }

    public static Set<String> fromHeaderValue(String headerValue)
    {
        if (headerValue == null) {
            return ImmutableSet.of();
        }
        return ImmutableSet.copyOf(SPLITTER.splitToList(headerValue));
    }
}

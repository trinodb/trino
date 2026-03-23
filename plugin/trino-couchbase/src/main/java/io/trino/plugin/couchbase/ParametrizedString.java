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
package io.trino.plugin.couchbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public record ParametrizedString(String text, List<Object> params)
{
    public static ParametrizedString from(String text)
    {
        return new ParametrizedString(text, Collections.emptyList());
    }

    public static ParametrizedString join(List<ParametrizedString> others, String delimeter, String before, String after)
    {
        List<Object> params = new ArrayList<>();
        return new ParametrizedString(
                others.stream()
                        .peek(ps -> {
                            params.addAll(ps.params());
                        })
                        .map(ParametrizedString::toString)
                        .collect(Collectors.joining(delimeter, before, after)),
                params);
    }

    public static ParametrizedString from(String s, List<Object> params)
    {
        return new ParametrizedString(
                s, params);
    }

    @Override
    public String toString()
    {
        return text;
    }
}

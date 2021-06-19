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
package io.trino.util;

import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.Type;

import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.lang.String.format;

public final class DynamicFiltersTestUtil
{
    private DynamicFiltersTestUtil() {}

    public static String getSimplifiedDomainString(long low, long high, int rangeCount, Type type)
    {
        /* Expected simplified domain string is generated explicitly here instead of
         * directly relying on output of Domain#toString so that we know when formatting or content
         * of the output changes in some unexpected way. It is necessary to ensure that the output
         * remains readable and compact while providing sufficient information.
         * This information is provided to user as part of EXPLAIN ANALYZE.
         */
        String formattedValues;
        if (rangeCount == 1) {
            formattedValues = format("{[%d]}", low);
        }
        else if (rangeCount == 2) {
            formattedValues = LongStream.of(low, high)
                    .mapToObj(value -> "[" + value + "]")
                    .collect(Collectors.joining(", ", "{", "}"));
        }
        else {
            formattedValues = format("{[%d], ..., [%d]}", low, high);
        }
        return "[ " +
                new StringJoiner(", ", SortedRangeSet.class.getSimpleName() + "[", "]")
                        .add("type=" + type)
                        .add("ranges=" + rangeCount)
                        .add(formattedValues) +
                " ]";
    }
}

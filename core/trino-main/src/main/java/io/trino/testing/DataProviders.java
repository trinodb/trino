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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.DataProvider;

import java.util.Arrays;
import java.util.stream.Collector;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public final class DataProviders
{
    private DataProviders() {}

    public static <T> Collector<T, ?, Object[][]> toDataProvider()
    {
        return collectingAndThen(
                mapping(
                        value -> new Object[] {value},
                        toList()),
                list -> list.toArray(new Object[][] {}));
    }

    @DataProvider
    public static Object[][] trueFalse()
    {
        return new Object[][] {{true}, {false}};
    }

    /**
     * @return Full cartesian product of arguments, i.e. cartesianProduct({{A, B}}, {{1,2}}) will produce {{A,1}, {A,2}, {B,1}, {B,2}}
     */
    public static Object[][] cartesianProduct(Object[][]... args)
    {
        return Lists.cartesianProduct(Arrays.stream(args)
                        .map(ImmutableList::copyOf)
                        .collect(toImmutableList()))
                .stream()
                .map(list -> list.stream()
                        .flatMap(Stream::of)
                        .toArray(Object[]::new))
                .toArray(Object[][]::new);
    }

    /**
     * @return args concatenated together into a single Object[][]
     */
    public static Object[][] concat(Object[][]... args)
    {
        return Arrays.stream(args)
                .flatMap(Arrays::stream)
                .toArray(Object[][]::new);
    }
}

/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.toolkit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestNGUtils
{
    private TestNGUtils() {}

    /**
     * @return Full cartesian product of arguments, i.e cartesianProduct({A, B}, {1,2}) will produce {{A,1}, {A,2}, {B,1}, {B,2}}
     */
    public static Object[][] cartesianProduct(Object[]... args)
    {
        return Lists.cartesianProduct(Arrays.stream(args)
                .map(ImmutableList::copyOf)
                .collect(toImmutableList()))
                .stream()
                .map(list -> list.toArray(Object[]::new))
                .toArray(Object[][]::new);
    }

    /**
     * @return Full cartesian product of the arguments
     * i.e cartesianProduct({{A,1}, {B,2}}, {{C,3},{D,4}}) will produce
     * {{A,1,C,3}, {A,1,D,4}, {B,2,C,3}, {B,2,,D,4}}
     */
    public static Object[][] cartesianProduct(Object[][] first, Object[][] second)
    {
        List<Object[]> result = new ArrayList<>();
        for (Object[] arg1 : first) {
            for (Object[] arg2 : second) {
                result.add(Streams.concat(Arrays.stream(arg1), Arrays.stream(arg2)).toArray());
            }
        }

        return result.toArray(Object[][]::new);
    }

    /**
     * @return args concatenated together into a single Object[][]
     */
    public static Object[][] combine(Object[][]... args)
    {
        return Arrays.stream(args)
                .flatMap(Arrays::stream)
                .toArray(Object[][]::new);
    }

    /**
     * Combines arguments into a single Object array.
     * If any of the arguments is an Object array itself, it is flattened
     */
    public static Object[] combineAndFlatten(Object... args)
    {
        return Arrays.stream(args)
                .flatMap(arg -> arg instanceof Object[] ? Arrays.stream((Object[]) arg) : Stream.of(arg))
                .toArray();
    }
}

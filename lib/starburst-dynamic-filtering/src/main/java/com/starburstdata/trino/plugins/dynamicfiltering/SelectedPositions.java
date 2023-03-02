/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SelectedPositions
{
    public static final SelectedPositions EMPTY = positionsRange(0);

    private final boolean isList;
    private final int[] positions;
    private final int size;

    public static SelectedPositions positionsList(int[] positions, int size)
    {
        return new SelectedPositions(true, positions, size);
    }

    public static SelectedPositions positionsRange(int size)
    {
        return new SelectedPositions(false, new int[0], size);
    }

    private SelectedPositions(boolean isList, int[] positions, int size)
    {
        this.isList = isList;
        this.positions = requireNonNull(positions, "positions is null");
        this.size = size;

        checkArgument(size >= 0, "size is negative");
        if (isList) {
            checkPositionIndexes(0, size, positions.length);
        }
    }

    public boolean isList()
    {
        return isList;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int[] getPositions()
    {
        checkState(isList, "SelectedPositions is a range");
        return positions;
    }

    public int size()
    {
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SelectedPositions other = (SelectedPositions) o;
        return isList == other.isList &&
                size == other.size &&
                (!isList || Arrays.equals(positions, 0, size, other.positions, 0, other.size));
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(isList, size);
        result = 31 * result + Arrays.hashCode(positions);
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("isList", isList)
                .add("positions", positions)
                .add("size", size)
                .toString();
    }
}

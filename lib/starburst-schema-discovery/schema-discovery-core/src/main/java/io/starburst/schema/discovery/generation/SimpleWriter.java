/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

import java.util.Deque;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkState;

@SuppressWarnings("UnusedReturnValue")
class SimpleWriter
{
    private final StringBuilder str = new StringBuilder();
    private int indentLevel;
    private boolean isOnNewLine = true;
    private final Deque<Boolean> listLevelWithFirstItem = new LinkedList<>();

    SimpleWriter startList()
    {
        listLevelWithFirstItem.addFirst(false);
        return this;
    }

    SimpleWriter endList()
    {
        return endList(true);
    }

    SimpleWriter endList(boolean emptyLineAtTheEnd)
    {
        checkState(!listLevelWithFirstItem.isEmpty(), "not in list");
        if (listLevelWithFirstItem.pop() && emptyLineAtTheEnd) {
            println();
        }
        return this;
    }

    @FormatMethod
    SimpleWriter addToList(@FormatString final String format, Object... args)
    {
        checkState(!listLevelWithFirstItem.isEmpty(), "not in list");
        if (listLevelWithFirstItem.peek()) {
            println(",");
        }
        print(format, args);
        listLevelWithFirstItem.pop();
        listLevelWithFirstItem.addFirst(true);
        return this;
    }

    @FormatMethod
    SimpleWriter print(@FormatString String format, Object... args)
    {
        addIndent();
        str.append(String.format(format, args));
        return this;
    }

    @FormatMethod
    SimpleWriter println(@FormatString final String format, Object... args)
    {
        print(format, args);
        println();
        return this;
    }

    SimpleWriter indent()
    {
        ++indentLevel;
        return this;
    }

    SimpleWriter outdent()
    {
        checkState(indentLevel > 0, "indent/outdent mismatch");
        --indentLevel;
        return this;
    }

    SimpleWriter println()
    {
        str.append('\n');
        isOnNewLine = true;
        return this;
    }

    @Override
    public String toString()
    {
        return str.toString();
    }

    private void addIndent()
    {
        if (isOnNewLine) {
            isOnNewLine = false;
            str.append("    ".repeat(Math.max(0, indentLevel)));
        }
    }
}

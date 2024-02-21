/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LineStreamSampler<T>
{
    private final Supplier<Optional<T>> nextLineSupplier;
    private final int maxLines;
    private final int modulo;

    public LineStreamSampler(int maxLines, int modulo, Supplier<Optional<T>> nextLineSupplier)
    {
        this.maxLines = maxLines;
        this.modulo = modulo;
        this.nextLineSupplier = nextLineSupplier;
    }

    public Stream<T> stream()
    {
        var iterator = new Iterator<T>()
        {
            private int count;
            private int lineNumber;
            private T next;

            @Override
            public boolean hasNext()
            {
                return next != null;
            }

            @Override
            public T next()
            {
                T thisNext = this.next;
                setNext();
                return thisNext;
            }

            private void setNext()
            {
                next = null;
                while (count < maxLines) {
                    Optional<T> strings = nextLineSupplier.get();
                    if (strings.isEmpty()) {
                        break;
                    }
                    ++lineNumber;   // lineNumber is 1 based
                    if ((lineNumber == 1) || ((lineNumber % modulo) == 0)) {    // always sample the first line in case there aren't enough lines
                        ++count;
                        next = strings.get();
                        break;
                    }
                }
            }
        };
        iterator.setNext();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }
}

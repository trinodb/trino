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

import com.google.common.base.Splitter;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.IO;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TextFileLines
{
    private static final Pattern CRLF = Pattern.compile("[\\r\\n]");
    public static final int MAX_GUESS_BUFFER_SIZE = 1024;

    public enum TestLinesMode
    {
        ALL_MATCH(Stream::allMatch),
        ANY_MATCH(Stream::anyMatch);

        private final BiFunction<Stream<String>, Predicate<String>, Boolean> matcher;

        TestLinesMode(BiFunction<Stream<String>, Predicate<String>, Boolean> matcher)
        {
            this.matcher = matcher;
        }
    }

    public static boolean testLines(byte[] bytes, TestLinesMode mode, Predicate<String> test)
    {
        List<String> lines = Splitter.on(CRLF).trimResults().omitEmptyStrings().splitToList(new String(bytes, UTF_8));
        return switch (lines.size()) {
            case 0 -> false;
            case 1 -> mode.matcher.apply(lines.stream(), test);
            // if there is more than 1 line, skip 1st line as it's probably headers
            case 2 -> mode.matcher.apply(lines.subList(1, lines.size()).stream(), test);
            // ignore last line as it's likely not a complete line
            default -> mode.matcher.apply(lines.subList(1, lines.size() - 1).stream(), test);
        };
    }

    public static boolean testLine(byte[] bytes, TestLinesMode mode, Predicate<String> test)
    {
        return mode.matcher.apply(Stream.of(new String(bytes, UTF_8)), test);
    }

    public static Optional<byte[]> getTestBytes(DiscoveryInput stream)
    {
        try {
            stream.rewind();
            byte[] bytes = new byte[MAX_GUESS_BUFFER_SIZE];
            int bytesRead = stream.asInputStream().read(bytes);
            stream.rewind();
            if (bytesRead == bytes.length) {
                return Optional.of(bytes);
            }
            if (bytesRead > 0) {
                return Optional.of(Arrays.copyOf(bytes, bytesRead));
            }
            return Optional.empty();
        }
        catch (IOException e) {
            throw new TrinoException(IO, e);
        }
    }

    private TextFileLines()
    {
    }
}

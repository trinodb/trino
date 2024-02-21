/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.io;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static io.starburst.schema.discovery.internal.TextFileLines.MAX_GUESS_BUFFER_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBufferedResettableInputStream
{
    @Test
    public void testResetRereadsSameInitialBytesBigSource()
            throws IOException
    {
        byte[] sourceBuffer = Strings.repeat("qwerty", 500).getBytes(UTF_8);
        assertThat(sourceBuffer.length).isGreaterThan(MAX_GUESS_BUFFER_SIZE);

        assertRepeatableInputStream(sourceBuffer);
    }

    @Test
    public void testResetRereadsSameInitialBytesSmallSource()
            throws IOException
    {
        byte[] sourceBuffer = Strings.repeat("qwerty", 5).getBytes(UTF_8);
        assertThat(sourceBuffer.length).isLessThan(MAX_GUESS_BUFFER_SIZE);

        assertRepeatableInputStream(sourceBuffer);
    }

    private static void assertRepeatableInputStream(byte[] sourceBuffer)
            throws IOException
    {
        byte[] buffer1 = new byte[MAX_GUESS_BUFFER_SIZE];
        byte[] buffer2 = new byte[MAX_GUESS_BUFFER_SIZE];
        byte[] buffer3 = new byte[MAX_GUESS_BUFFER_SIZE];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(sourceBuffer);
        try (BufferedResettableInputStream resetable = new BufferedResettableInputStream(byteArrayInputStream)) {
            resetable.read(buffer1);
            resetable.seek(0);
            resetable.read(buffer2);
            resetable.seek(0);
            resetable.read(buffer3);
        }
        assertThat(buffer1).isEqualTo(buffer2).isEqualTo(buffer3);
    }
}

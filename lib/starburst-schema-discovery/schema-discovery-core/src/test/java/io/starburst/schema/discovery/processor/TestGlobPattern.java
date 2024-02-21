/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import com.google.re2j.PatternSyntaxException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestGlobPattern
{
    @Test
    public void testValidPatterns()
    {
        assertMatching(true, "*", "^$", "foo", "bar", "\n");
        assertMatching(true, "?", "?", "^", "[", "]", "$");
        assertMatching(true, "foo*", "foo", "food", "fool", "foo\n", "foo\nbar");
        assertMatching(true, "f*d", "fud", "food", "foo\nd");
        assertMatching(true, "*d", "good", "bad", "\nd");
        assertMatching(true, "\\*\\?\\[\\{\\\\", "*?[{\\");
        assertMatching(true, "[]^-]", "]", "-", "^");
        assertMatching(true, "]", "]");
        assertMatching(true, "^.$()|+", "^.$()|+");
        assertMatching(true, "[^^]", ".", "$", "[", "]");
        assertMatching(false, "[^^]", "^");
        assertMatching(true, "[!!-]", "^", "?");
        assertMatching(false, "[!!-]", "!", "-");
        assertMatching(true, "{[12]*,[45]*,[78]*}", "1", "2!", "4", "42", "7", "7$");
        assertMatching(false, "{[12]*,[45]*,[78]*}", "3", "6", "9ß");
        assertMatching(true, "}", "}");
    }

    @Test
    public void testInvalidPatterns()
    {
        shouldThrow("[", "[[]]", "{", "\\");
    }

    @Test
    @Timeout(10)
    public void testPathologicalPatterns()
    {
        String badFilename = "job_1429571161900_4222-1430338332599-tda%2D%2D+******************************+++...%270%27%28Stage-1430338580443-39-2000-SUCCEEDED-production%2Dhigh-1430338340360.jhist";
        assertMatching(true, badFilename, badFilename);
    }

    private void assertMatching(boolean expectedMatch, String glob, String... input)
    {
        GlobPattern pattern = new GlobPattern(glob);

        for (String s : input) {
            if (expectedMatch) {
                assertThat(pattern.matches(s))
                        .withFailMessage("%s should match %s", glob, s)
                        .isTrue();
            }
            else {
                assertThat(pattern.matches(s))
                        .withFailMessage("%s should not match %s", glob, s)
                        .isFalse();
            }
        }
    }

    private void shouldThrow(String... globs)
    {
        for (String glob : globs) {
            try {
                new GlobPattern(glob);
            }
            catch (PatternSyntaxException expected) {
                continue;
            }
            fail("glob " + glob + " should throw");
        }
    }
}

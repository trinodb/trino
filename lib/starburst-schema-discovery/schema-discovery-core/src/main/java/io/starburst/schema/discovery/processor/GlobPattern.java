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

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

/**
 * A class for POSIX glob pattern with brace expansions.
 * <p>
 * Note: copied from {@code org.apache.hadoop.fs.GlobPattern} as {@code org.apache.hadoop} dependency
 * was removed and this class isn't available anymore. That's only leftover from hadoop which is
 * needed in schema discovery, and there is no alternative in Trino
 */
public class GlobPattern
{
    private static final char BACKSLASH = '\\';

    private final Pattern compiled;

    /**
     * Construct the glob pattern object with a glob pattern string
     *
     * @param globPattern the glob pattern string
     */
    public GlobPattern(String globPattern)
    {
        compiled = compile(globPattern);
    }

    /**
     * Match input against the compiled glob pattern
     *
     * @return true for successful matches
     */
    public boolean matches(String s)
    {
        return compiled.matcher(s).matches();
    }

    private static Pattern compile(String glob)
    {
        StringBuilder regex = new StringBuilder();
        int setOpen = 0;
        int curlyOpen = 0;
        int len = glob.length();

        for (int i = 0; i < len; i++) {
            char c = glob.charAt(i);

            switch (c) {
                case BACKSLASH:
                    if (++i >= len) {
                        error("Missing escaped character", glob, i);
                    }
                    regex.append(c).append(glob.charAt(i));
                    continue;
                case '.':
                case '$':
                case '(':
                case ')':
                case '|':
                case '+':
                    // escape regex special chars that are not glob special chars
                    regex.append(BACKSLASH);
                    break;
                case '*':
                    regex.append('.');
                    break;
                case '?':
                    regex.append('.');
                    continue;
                case '{': // start of a group
                    regex.append("(?:"); // non-capturing
                    curlyOpen++;
                    continue;
                case ',':
                    regex.append(curlyOpen > 0 ? '|' : c);
                    continue;
                case '}':
                    if (curlyOpen > 0) {
                        // end of a group
                        curlyOpen--;
                        regex.append(")");
                        continue;
                    }
                    break;
                case '[':
                    if (setOpen > 0) {
                        error("Unclosed character class", glob, i);
                    }
                    setOpen++;
                    break;
                case '^': // ^ inside [...] can be unescaped
                    if (setOpen == 0) {
                        regex.append(BACKSLASH);
                    }
                    break;
                case '!': // [! needs to be translated to [^
                    regex.append(setOpen > 0 && '[' == glob.charAt(i - 1) ? '^' : '!');
                    continue;
                case ']':
                    // Many set errors like [][] could not be easily detected here,
                    // as []], []-] and [-] are all valid POSIX glob and java regex.
                    // We'll just let the regex compiler do the real work.
                    setOpen = 0;
                    break;
                default:
            }
            regex.append(c);
        }

        if (setOpen > 0) {
            error("Unclosed character class", glob, len);
        }
        if (curlyOpen > 0) {
            error("Unclosed group", glob, len);
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }

    private static void error(String message, String pattern, int pos)
    {
        String fullMessage = String.format("%s at pos %d", message, pos);
        throw new PatternSyntaxException(fullMessage, pattern);
    }
}

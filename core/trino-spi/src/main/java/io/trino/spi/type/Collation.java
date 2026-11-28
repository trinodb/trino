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
package io.trino.spi.type;

import static java.util.Objects.requireNonNull;

/// A rule for comparing character strings: a named comparison and a pad attribute.
///
/// A collation is a value: it names a comparison but does not implement it. The default
/// collation, [#UCS_BASIC], orders strings by Unicode code point, which for valid UTF-8 is the
/// order of their bytes. The engine implements every other collation, and plugins see collations
/// only as part of character string types.
///
/// A `PAD SPACE` collation compares the shorter of two strings as though it were extended with
/// spaces to the length of the longer, so trailing spaces do not affect equality, and a string
/// followed by a character that sorts below a space, such as a tab, sorts below the string. A
/// `NO PAD` collation treats every character as significant. Two collations are the same
/// collation exactly when their names and pad attributes are equal.
public final class Collation
{
    static final String UCS_BASIC_NAME = "ucs_basic";
    // the suffix of the descriptor of a PAD SPACE collation, as in "unicode_ci pad space"
    private static final String PAD_SPACE_SUFFIX = " pad space";

    public static final Collation UCS_BASIC = new Collation(UCS_BASIC_NAME, false);

    private final String name;
    private final boolean padSpace;

    private Collation(String name, boolean padSpace)
    {
        this.name = requireNonNull(name, "name is null");
        this.padSpace = padSpace;
    }

    /// The collation with the given canonical name, which is a lower-case letter followed by
    /// lower-case letters, digits, or underscores. Whether the engine implements the collation is
    /// decided when a type is resolved.
    public static Collation of(String name)
    {
        requireNonNull(name, "name is null");
        if (name.equals(UCS_BASIC_NAME)) {
            return UCS_BASIC;
        }
        if (!isValidName(name)) {
            throw new IllegalArgumentException("Invalid collation name: " + name);
        }
        return new Collation(name, false);
    }

    /// The collation a type descriptor records, as produced by [#descriptor()].
    public static Collation fromDescriptor(String descriptor)
    {
        if (descriptor.endsWith(PAD_SPACE_SUFFIX)) {
            return of(descriptor.substring(0, descriptor.length() - PAD_SPACE_SUFFIX.length())).withPadSpace();
        }
        return of(descriptor);
    }

    // Whether a name is a canonical collation name: a lower-case letter followed by lower-case
    // letters, digits, or underscores.
    static boolean isValidName(String name)
    {
        if (name.isEmpty() || name.charAt(0) < 'a' || name.charAt(0) > 'z') {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
                return false;
            }
        }
        return true;
    }

    /// The name of this collation's comparison, without its pad attribute.
    public String name()
    {
        return name;
    }

    /// Whether this collation compares strings as though the shorter were padded with spaces.
    public boolean isPadSpace()
    {
        return padSpace;
    }

    /// This collation's comparison with the `PAD SPACE` attribute.
    public Collation withPadSpace()
    {
        return padSpace ? this : new Collation(name, true);
    }

    /// This collation's comparison with the `NO PAD` attribute.
    public Collation withNoPad()
    {
        return padSpace ? of(name) : this;
    }

    /// The canonical descriptor of this collation: its name, followed by ` pad space` for a
    /// `PAD SPACE` collation. Types record a collation by its descriptor.
    public String descriptor()
    {
        return padSpace ? name + PAD_SPACE_SUFFIX : name;
    }

    /// Whether this is the default, code-point order `NO PAD` collation.
    public boolean isDefault()
    {
        return name.equals(UCS_BASIC_NAME) && !padSpace;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof Collation that && name.equals(that.name) && padSpace == that.padSpace;
    }

    @Override
    public int hashCode()
    {
        return descriptor().hashCode();
    }

    @Override
    public String toString()
    {
        return descriptor();
    }
}

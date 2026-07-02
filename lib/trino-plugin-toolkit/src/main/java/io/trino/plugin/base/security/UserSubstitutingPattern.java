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
package io.trino.plugin.base.security;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.util.Objects.requireNonNull;

/**
 * A pattern that may contain the literal placeholder {@code {user}}.
 * <p>
 * If the raw string contains {@code {user}}, compilation is deferred to match
 * time and the placeholder is replaced with {@link Pattern#quote(String)} of
 * the authenticated username before compiling.  If the raw string does not
 * contain {@code {user}} it is compiled once at deserialise time, preserving
 * the existing behaviour exactly.
 */
public final class UserSubstitutingPattern
{
    private static final String USER_PLACEHOLDER = "{user}";

    /** Non-null when the raw string contained no {@code {user}} placeholder. */
    private final Pattern compiled;
    /** Non-null when the raw string contained a {@code {user}} placeholder. */
    private final String template;

    private UserSubstitutingPattern(Pattern compiled, String template)
    {
        this.compiled = compiled;
        this.template = template;
    }

    public static UserSubstitutingPattern of(String raw)
    {
        requireNonNull(raw, "raw is null");
        if (raw.contains(USER_PLACEHOLDER)) {
            return new UserSubstitutingPattern(null, raw);
        }
        return new UserSubstitutingPattern(Pattern.compile(raw), null);
    }

    /**
     * Returns {@code true} if {@code value} matches this pattern after
     * substituting {@code {user}} with the authenticated {@code user}.
     */
    public boolean matches(String user, String value)
    {
        Pattern p = (template != null)
                ? Pattern.compile(template.replace(USER_PLACEHOLDER, Pattern.quote(user)))
                : compiled;
        return p.matcher(value).matches();
    }

    /**
     * Returns {@code true} if this pattern contains a {@code {user}}
     * placeholder and therefore cannot be evaluated without a specific user.
     */
    public boolean hasUserPlaceholder()
    {
        return template != null;
    }

    /** Returns the raw pattern string as written in rules.json. */
    public String raw()
    {
        return template != null ? template : compiled.pattern();
    }

    @Override
    public String toString()
    {
        return raw();
    }

    // -------------------------------------------------------------------------
    // Jackson deserializer
    // -------------------------------------------------------------------------

    public static final class Deserializer
            extends StdDeserializer<UserSubstitutingPattern>
    {
        public Deserializer()
        {
            super(UserSubstitutingPattern.class);
        }

        @Override
        public UserSubstitutingPattern deserialize(JsonParser p, DeserializationContext ctx)
                throws IOException
        {
            String raw = p.getText();
            if (!raw.contains(USER_PLACEHOLDER)) {
                // Validate that it is a legal regex right now (fail-fast, same as before).
                try {
                    Pattern.compile(raw);
                }
                catch (PatternSyntaxException e) {
                    throw new IOException("Invalid regex pattern '" + raw + "': " + e.getMessage(), e);
                }
            }
            return UserSubstitutingPattern.of(raw);
        }
    }
}

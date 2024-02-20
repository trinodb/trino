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
package io.trino.grammar.sql;

import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.Vocabulary;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.nullToEmpty;

public final class SqlKeywords
{
    private static final Pattern IDENTIFIER = Pattern.compile("'([A-Z_]+)'");

    private SqlKeywords() {}

    public static Set<String> sqlKeywords()
    {
        ImmutableSet.Builder<String> names = ImmutableSet.builder();
        Vocabulary vocabulary = SqlBaseLexer.VOCABULARY;
        for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
            String name = nullToEmpty(vocabulary.getLiteralName(i));
            Matcher matcher = IDENTIFIER.matcher(name);
            if (matcher.matches()) {
                names.add(matcher.group(1));
            }
        }
        return names.build();
    }
}

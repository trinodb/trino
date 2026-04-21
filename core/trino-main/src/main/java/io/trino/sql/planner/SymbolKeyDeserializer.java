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
package io.trino.sql.planner;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.google.common.base.CharMatcher;
import com.google.inject.Inject;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

public class SymbolKeyDeserializer
        extends KeyDeserializer
{
    private static final CharMatcher DIGIT_MATCHER = CharMatcher.inRange('0', '9').precomputed();

    private final TypeManager typeManager;

    @Inject
    public SymbolKeyDeserializer(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext context)
    {
        int keyLength = key.length();

        // Shortest valid key is "1|n|t", which has length 5
        checkArgument(keyLength > 4, "Symbol key is malformed: %s", key);

        int lastDigitIndex = getLeadingDigitsLength(key, keyLength);
        checkArgument(lastDigitIndex > 0, "Symbol key is malformed: %s", key);

        int length = parseInt(key.substring(0, lastDigitIndex));
        checkArgument(lastDigitIndex + length + 2 < keyLength, "Symbol key is malformed: %s", key);

        String type = key.substring(lastDigitIndex + 1, lastDigitIndex + length + 1);
        String name = key.substring(lastDigitIndex + length + 2);
        return new Symbol(typeManager.getType(TypeId.of(type)), name);
    }

    public static int getLeadingDigitsLength(String input, int length)
    {
        int index = 0;
        while (index < length && DIGIT_MATCHER.matches(input.charAt(index))) {
            index++;
        }
        return index;
    }
}

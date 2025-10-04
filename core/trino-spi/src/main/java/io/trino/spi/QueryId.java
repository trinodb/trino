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
package io.trino.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record QueryId(String id)
{
    private static final int INSTANCE_SIZE = instanceSize(QueryId.class);

    @JsonCreator
    public static QueryId valueOf(String queryId)
    {
        // ID is verified in the constructor
        return new QueryId(queryId);
    }

    public QueryId
    {
        requireNonNull(id, "id is null");
        checkArgument(!id.isEmpty(), "id is empty");
        validateId(id);
    }

    // For backward compatibility
    @JsonValue
    @Deprecated // Use id() instead
    public String getId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return id;
    }

    //
    // Id helper methods
    //
    // Check if the string matches [_a-z0-9]+ , but without the overhead of regex
    private static boolean isValidId(String id)
    {
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            if (!(c == '_' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9')) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidDottedId(char[] chars)
    {
        for (int i = 0; i < chars.length; i++) {
            if (!(chars[i] == '_' || chars[i] == '.' || chars[i] >= 'a' && chars[i] <= 'z' || chars[i] >= '0' && chars[i] <= '9')) {
                return false;
            }
        }
        return true;
    }

    public static String validateId(String id)
    {
        if (!isValidId(id)) {
            throw new IllegalArgumentException("Invalid queryId " + id);
        }
        return id;
    }

    public static List<String> parseDottedId(String id, int expectedParts, String name)
    {
        requireNonNull(id, "id is null");
        checkArgument(expectedParts > 1, "expectedParts must be at least 2");
        requireNonNull(name, "name is null");

        char[] chars = id.toCharArray();
        if (!isValidDottedId(chars)) {
            throw new IllegalArgumentException("Invalid " + name + " " + id);
        }
        String[] parts = new String[expectedParts];
        int startOffset = 0;
        int partIndex = 0;
        for (int i = 0, length = chars.length; i < length; i++) {
            if (chars[i] == '.') {
                if (i <= startOffset || i == length - 1) {
                    throw new IllegalArgumentException("Invalid " + name + " " + id);
                }
                parts[partIndex++] = new String(chars, startOffset, i - startOffset);
                startOffset = i + 1;
            }
        }
        parts[partIndex++] = new String(chars, startOffset, chars.length - startOffset);
        if (partIndex != expectedParts) {
            throw new IllegalArgumentException("Invalid " + name + " " + id);
        }
        return List.of(parts);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(id);
    }
}

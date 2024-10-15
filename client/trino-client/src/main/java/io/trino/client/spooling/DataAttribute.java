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
package io.trino.client.spooling;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public enum DataAttribute
{
    // Offset of the segment in relation to the whole result set
    ROW_OFFSET("rowOffset", Long.class),
    // Number of rows in the segment
    ROWS_COUNT("rowsCount", Long.class),
    // Actual size of the segment in bytes (uncompressed or compressed)
    SEGMENT_SIZE("segmentSize", Integer.class),
    // Size of the segment in bytes after decompression, added only to compressed segments
    UNCOMPRESSED_SIZE("uncompressedSize", Integer.class),
    // Placeholder for future encoder-specific schema
    SCHEMA("schema", String.class);

    private final String name;
    private final Class<?> javaClass;

    DataAttribute(String name, Class<?> javaClass)
    {
        this.name = requireNonNull(name, "name is null");
        this.javaClass = requireNonNull(javaClass, "javaClass is null");
    }

    public String attributeName()
    {
        return name;
    }

    public Class<?> javaClass()
    {
        return javaClass;
    }

    public static DataAttribute getByName(String name)
    {
        for (DataAttribute attributeName : DataAttribute.values()) {
            if (attributeName.attributeName().equals(name)) {
                return attributeName;
            }
        }
        throw new IllegalArgumentException("Unknown attribute name: " + name);
    }

    public <T> T decode(Class<T> clazz, Object value)
    {
        verify(clazz == javaClass, "Expected %s, but got %s", javaClass, clazz);
        if (clazz == Long.class) {
            if (value instanceof Long) {
                return clazz.cast(value);
            }
            if (value instanceof Integer) {
                return clazz.cast(Integer.class.cast(value).longValue());
            }
            if (value instanceof String) {
                return clazz.cast(Long.parseLong(String.class.cast(value)));
            }
        }

        if (clazz == Integer.class) {
            if (value instanceof Long) {
                return clazz.cast(toIntExact(Long.class.cast(value)));
            }
            if (value instanceof Integer) {
                return clazz.cast(value);
            }
            if (value instanceof String) {
                return clazz.cast(Integer.parseInt(String.class.cast(value)));
            }
        }

        if (clazz == String.class) {
            if (value instanceof String) {
                return clazz.cast(value);
            }
        }

        if (clazz == Boolean.class) {
            if (value instanceof Boolean) {
                return clazz.cast(value);
            }

            if (value instanceof String) {
                return clazz.cast(Boolean.parseBoolean(String.class.cast(value)));
            }
        }

        throw new IllegalArgumentException("Unsupported class: " + clazz);
    }
}

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
package io.trino.client.uri;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.assertj.core.api.Assertions.assertThat;

class TestConnectionProperties
{
    @Test
    public void testAllPropertiesCollected()
    {
        Set<Object> properties = Arrays.stream(ConnectionProperties.class.getDeclaredFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()) && Modifier.isPublic(field.getModifiers()))
                .map(TestConnectionProperties::getFieldValue)
                .collect(toImmutableSet());

        assertThat(properties).isEqualTo(ConnectionProperties.allProperties());
    }

    private static Object getFieldValue(Field field)
    {
        try {
            return field.get(null);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}

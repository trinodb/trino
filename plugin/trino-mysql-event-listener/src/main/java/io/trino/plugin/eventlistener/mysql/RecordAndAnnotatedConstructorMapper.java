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
package io.trino.plugin.eventlistener.mysql;

import org.jdbi.v3.core.config.ConfigRegistry;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.mapper.RowMapperFactory;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.stream.Stream;

public class RecordAndAnnotatedConstructorMapper
        implements RowMapperFactory
{
    @Override
    public Optional<RowMapper<?>> build(Type type, ConfigRegistry config)
    {
        if ((type instanceof Class<?> clazz) && (clazz.isRecord() || hasJdbiConstructorMethod(clazz))) {
            return ConstructorMapper.factory(clazz).build(type, config);
        }
        return Optional.empty();
    }

    private static boolean hasJdbiConstructorMethod(Class<?> clazz)
    {
        return Stream.of(clazz.getConstructors()).anyMatch(constructor -> (constructor.getAnnotation(JdbiConstructor.class) != null))
                || Stream.of(clazz.getMethods()).anyMatch(method -> (method.getAnnotation(JdbiConstructor.class) != null));
    }
}

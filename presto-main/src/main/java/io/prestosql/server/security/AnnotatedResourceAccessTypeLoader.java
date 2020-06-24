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
package io.prestosql.server.security;

import io.prestosql.server.security.ResourceSecurity.AccessType;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

public class AnnotatedResourceAccessTypeLoader
        implements ResourceAccessTypeLoader
{
    @Override
    public Optional<AccessType> getAccessType(AnnotatedElement element)
    {
        return Optional.ofNullable(element.getAnnotation(ResourceSecurity.class))
                .map(ResourceSecurity::value);
    }
}

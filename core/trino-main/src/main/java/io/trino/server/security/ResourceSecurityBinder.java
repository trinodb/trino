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
package io.trino.server.security;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;
import io.trino.server.security.ResourceSecurity.AccessType;

import java.lang.reflect.AnnotatedElement;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class ResourceSecurityBinder
{
    private final MapBinder<AnnotatedElement, AccessType> accessTypeBinder;

    public static ResourceSecurityBinder resourceSecurityBinder(Binder binder)
    {
        return new ResourceSecurityBinder(binder);
    }

    private ResourceSecurityBinder(Binder binder)
    {
        requireNonNull(binder, "binder is null");
        binder.bind(ResourceAccessType.class).in(SINGLETON);
        binder.bind(StaticResourceAccessTypeLoader.class).in(SINGLETON);
        accessTypeBinder = MapBinder.newMapBinder(binder, AnnotatedElement.class, AccessType.class);
    }

    public ResourceSecurityBinder resourceSecurity(AnnotatedElement element, AccessType accessType)
    {
        accessTypeBinder.addBinding(element).toInstance(accessType);
        return this;
    }

    public ResourceSecurityBinder publicResource(AnnotatedElement element)
    {
        return resourceSecurity(element, AccessType.PUBLIC);
    }

    public ResourceSecurityBinder anyUserResource(AnnotatedElement element)
    {
        return resourceSecurity(element, AccessType.AUTHENTICATED_USER);
    }

    public ResourceSecurityBinder managementReadResource(AnnotatedElement element)
    {
        return resourceSecurity(element, AccessType.MANAGEMENT_READ);
    }

    public ResourceSecurityBinder managementWriteResource(AnnotatedElement element)
    {
        return resourceSecurity(element, AccessType.MANAGEMENT_WRITE);
    }

    public ResourceSecurityBinder internalOnlyResource(AnnotatedElement element)
    {
        return resourceSecurity(element, AccessType.INTERNAL_ONLY);
    }

    public static class StaticResourceAccessTypeLoader
            implements ResourceAccessTypeLoader
    {
        private final Map<AnnotatedElement, AccessType> accessTypeMap;

        @Inject
        public StaticResourceAccessTypeLoader(Map<AnnotatedElement, AccessType> accessTypeMap)
        {
            this.accessTypeMap = ImmutableMap.copyOf(requireNonNull(accessTypeMap, "accessTypeMap is null"));
        }

        @Override
        public Optional<AccessType> getAccessType(AnnotatedElement element)
        {
            return Optional.ofNullable(accessTypeMap.get(element));
        }
    }
}

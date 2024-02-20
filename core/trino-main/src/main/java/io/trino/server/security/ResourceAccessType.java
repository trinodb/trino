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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.server.security.ResourceSecurity.AccessType;
import io.trino.server.security.ResourceSecurityBinder.StaticResourceAccessTypeLoader;
import jakarta.ws.rs.container.ResourceInfo;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.MANAGEMENT_READ;

public class ResourceAccessType
{
    private final List<ResourceAccessTypeLoader> resourceAccessTypeLoaders;

    @Inject
    public ResourceAccessType(StaticResourceAccessTypeLoader staticResourceAccessTypeLoader)
    {
        this.resourceAccessTypeLoaders = ImmutableList.<ResourceAccessTypeLoader>builder()
                .add(staticResourceAccessTypeLoader)
                .add(new AnnotatedResourceAccessTypeLoader())
                .build();
    }

    public AccessType getAccessType(ResourceInfo resourceInfo)
    {
        for (ResourceAccessTypeLoader resourceAccessTypeLoader : resourceAccessTypeLoaders) {
            // check if the method has an access type declared
            Optional<AccessType> accessType = resourceAccessTypeLoader.getAccessType(resourceInfo.getResourceMethod());
            if (accessType.isPresent()) {
                return accessType.get();
            }
            // check if the resource class has an access type declared for all methods
            accessType = resourceAccessTypeLoader.getAccessType(resourceInfo.getResourceClass());
            if (accessType.isPresent()) {
                verifyNotTrinoResource(resourceInfo);
                return accessType.get();
            }
            // in some cases there the resource is a nested class, so check the parent class
            // we currently only check one level, but we could handle multiple nesting levels if necessary
            if (resourceInfo.getResourceClass().getDeclaringClass() != null) {
                accessType = resourceAccessTypeLoader.getAccessType(resourceInfo.getResourceClass().getDeclaringClass());
                if (accessType.isPresent()) {
                    verifyNotTrinoResource(resourceInfo);
                    return accessType.get();
                }
            }
        }
        // Trino resources are required to have a declared access control
        verifyNotTrinoResource(resourceInfo);
        return MANAGEMENT_READ;
    }

    private static void verifyNotTrinoResource(ResourceInfo resourceInfo)
    {
        Method resourceMethod = resourceInfo.getResourceMethod();
        if (resourceMethod != null && resourceMethod.getDeclaringClass().getPackageName().startsWith("io.trino.")) {
            throw new IllegalArgumentException("Trino resource is not annotated with @" + ResourceSecurity.class.getSimpleName() + ": " + resourceInfo.getResourceMethod());
        }
    }
}

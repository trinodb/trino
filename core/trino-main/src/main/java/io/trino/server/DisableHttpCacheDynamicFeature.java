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
package io.trino.server;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.DynamicFeature;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.FeatureContext;

import java.lang.reflect.Method;

public class DisableHttpCacheDynamicFeature
        implements DynamicFeature
{
    @Override
    public void configure(ResourceInfo resourceInfo, FeatureContext context)
    {
        if (isCacheDisabled(resourceInfo.getResourceClass()) || isCacheDisabled(resourceInfo.getResourceMethod())) {
            context.register(new DisableCacheResponseFilter());
        }
    }

    private static boolean isCacheDisabled(Class<?> clazz)
    {
        return clazz.getAnnotation(DisableHttpCache.class) != null;
    }

    private static boolean isCacheDisabled(Method method)
    {
        return method.getAnnotation(DisableHttpCache.class) != null;
    }

    private static class DisableCacheResponseFilter
            implements ContainerResponseFilter
    {
        @Override
        public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
        {
            responseContext.getHeaders().add("X-Download-Options", "noopen");
            responseContext.getHeaders().add("Cache-Control", "no-cache, no-store, max-age=0");
            responseContext.getHeaders().add("Pragma", "no-cache");
            responseContext.getHeaders().add("Expires", "0");
        }
    }
}

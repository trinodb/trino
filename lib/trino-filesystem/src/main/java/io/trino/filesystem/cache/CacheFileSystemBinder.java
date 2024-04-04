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
package io.trino.filesystem.cache;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static java.util.Objects.requireNonNull;

public record CacheFileSystemBinder(Binder binder)
{
    public static CacheFileSystemBinder fileSystemCacheBinder(Binder binder)
    {
        return new CacheFileSystemBinder(binder);
    }

    public CacheFileSystemBinder
    {
        requireNonNull(binder, "binder is null");
    }

    @CanIgnoreReturnValue
    public CacheFileSystemBinder allowCacheOnCoordinator()
    {
        newOptionalBinder(binder, Key.get(boolean.class, AllowFilesystemCacheOnCoordinator.class)).setBinding().toInstance(true);
        return this;
    }

    @CanIgnoreReturnValue
    public CacheFileSystemBinder withCacheKeyProvider(Class<? extends CacheKeyProvider> cacheKeyProvider)
    {
        newOptionalBinder(binder, CacheKeyProvider.class).setBinding().to(cacheKeyProvider).in(Scopes.SINGLETON);
        return this;
    }
}

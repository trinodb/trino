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
package io.trino.plugin.hive.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CachingDirectoryListerModule
        implements Module
{
    private final Optional<DirectoryLister> directoryLister;

    @VisibleForTesting
    public CachingDirectoryListerModule(Optional<DirectoryLister> directoryLister)
    {
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
    }

    @Override
    public void configure(Binder binder)
    {
        if (directoryLister.isPresent()) {
            binder.bind(DirectoryLister.class).toInstance(directoryLister.get());
        }
        else {
            binder.bind(DirectoryLister.class).to(CachingDirectoryLister.class).in(Scopes.SINGLETON);
        }
    }
}

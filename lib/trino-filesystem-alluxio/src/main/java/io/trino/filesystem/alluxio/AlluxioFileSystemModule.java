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
package io.trino.filesystem.alluxio;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import static com.google.inject.Scopes.SINGLETON;

public class AlluxioFileSystemModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(AlluxioFileSystemFactory.class).in(SINGLETON);
    }

    @Provides
    @Singleton
    public FileSystem createAlluxioFileSystem()
    {
        FileSystemContext fsContext = FileSystemContext.create(Configuration.global());
        return FileSystem.Factory.create(fsContext);
    }
}

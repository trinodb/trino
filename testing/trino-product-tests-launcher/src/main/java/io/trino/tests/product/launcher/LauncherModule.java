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
package io.trino.tests.product.launcher;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.tests.product.launcher.docker.DockerFiles;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

public final class LauncherModule
        implements Module
{
    private final OutputStream outputStream;

    public LauncherModule(OutputStream outputStream)
    {
        this.outputStream = requireNonNull(outputStream, "outputStream is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DockerFiles.class).in(Scopes.SINGLETON);
        binder.bind(OutputStream.class).toInstance(outputStream);
    }

    @Provides
    @Singleton
    private PrintStream provideOutputStreamPrinter()
    {
        return new PrintStream(outputStream, true, StandardCharsets.UTF_8);
    }
}

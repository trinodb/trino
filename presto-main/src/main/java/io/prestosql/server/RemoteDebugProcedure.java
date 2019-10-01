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
package io.prestosql.server;

import com.google.inject.Inject;
import com.google.inject.Injector;
import org.weakref.jmx.Managed;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;

public class RemoteDebugProcedure
{
    private static final String CLASS_NAME = "RemoteDebug";

    private Injector injector;

    @Inject
    public RemoteDebugProcedure(Injector injector)
    {
        this.injector = requireNonNull(injector, "injector is null");
    }

    @Managed
    public String remoteDebug(String code)
            throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException
    {
        Path tempDir = createTempDirectory("remote_debug");
        Path javaFile = saveSource(tempDir, code);
        Path classFile = compileSource(javaFile);
        return runClass(classFile);
    }

    private Path saveSource(Path tempDir, String source)
            throws IOException
    {
        Path sourcePath = tempDir.resolve(CLASS_NAME + ".java");
        Files.write(sourcePath, source.getBytes(UTF_8));
        return sourcePath;
    }

    private Path compileSource(Path javaFile)
    {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
        return javaFile.getParent().resolve(CLASS_NAME + ".class");
    }

    private String runClass(Path classFile)
            throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        URL classUrl = classFile.getParent().toFile().toURI().toURL();
        try (URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] {classUrl})) {
            Class<?> clazz = Class.forName(CLASS_NAME, true, classLoader);
            Function<Injector, String> instance = (Function<Injector, String>) clazz.newInstance();
            return instance.apply(injector);
        }
    }
}

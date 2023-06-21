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
package io.trino.hive.functions;

import io.trino.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.ql.exec.FunctionRegistry.getFunctionInfo;

public class StaticHiveFunctionRegistry implements HiveFunctionRegistry {

    private final ClassLoader classLoader;

    @Inject
    public StaticHiveFunctionRegistry(@ForHiveFunction ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public Class<?> getClass(String functionName)
            throws ClassNotFoundException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return getFunctionInfo(functionName).getFunctionClass();
        }
        catch (SemanticException | NullPointerException e) {
            throw new ClassNotFoundException("Class of function " + functionName + " not found", e);
        }
    }
}

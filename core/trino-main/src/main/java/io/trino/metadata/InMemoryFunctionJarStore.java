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
package io.trino.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionJarDynamicManager.getJarNameBasedType;

public class InMemoryFunctionJarStore
        implements FunctionJarStore
{
    private final Map<String, FunctionJar> udfJarMap = new ConcurrentHashMap<>();

    @Override
    public List<FunctionJar> getJars()
    {
        return udfJarMap.values().stream().filter(FunctionJar::notInPluginDir).collect(toImmutableList());
    }

    @Override
    public void addJar(FunctionJar functionJar)
    {
        udfJarMap.put(functionJar.getJarName(), functionJar);
    }

    @Override
    public void dropJar(String jarName, GlobalFunctionCatalog globalFunctionCatalog)
    {
        FunctionJar functionJar = udfJarMap.get(jarName);
        if (null != functionJar) {
            globalFunctionCatalog.dropFunctions(functionJar.getFunctionIds());
            udfJarMap.remove(jarName);
        }
    }

    @Override
    public boolean exists(String jarName)
    {
        return udfJarMap.containsKey(getJarNameBasedType(jarName));
    }
}

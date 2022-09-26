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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static io.trino.metadata.FunctionJarDynamicManager.getJarNameBasedType;

public class FunctionJar
        implements Serializable
{
    private String jarName;
    private String jarUrl;
    private List<String> functionIds = new ArrayList<>();
    // if in plugin, no need manual reload
    private boolean inPluginDir;

    public FunctionJar()
    {
    }

    public FunctionJar(String jarUrl, List<String> functionIds, boolean inPluginDir)
    {
        this.jarUrl = jarUrl;
        this.jarName = getJarNameBasedType(jarUrl);
        this.functionIds = functionIds;
        this.inPluginDir = inPluginDir;
    }

    public boolean notInPluginDir()
    {
        return !inPluginDir;
    }

    public String getJarName()
    {
        return jarName;
    }

    public String getJarUrl()
    {
        return jarUrl;
    }

    public List<String> getFunctionIds()
    {
        return functionIds;
    }

    public boolean isInPluginDir()
    {
        return inPluginDir;
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metadata.FunctionJarDynamicManager.getJarNameBasedType;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

// file name --> jarName
// file content --> FunctionJar
public final class LocalFileFunctionJarStore
        implements FunctionJarStore
{
    private static final Logger log = Logger.get(LocalFileFunctionJarStore.class);

    private final boolean readOnly;
    private final File jarDirectory;
    // key -> jar name ==>  key -> udf jar url  | value -> udf functions
    private final Map<String, FunctionJar> udfJarMap = new ConcurrentHashMap<>();
    private final String jarMetaFileExtension = ".jar_meta";

    @Inject
    public LocalFileFunctionJarStore(LocalFileFunctionJarStoreConfig config)
    {
        requireNonNull(config, "jar meta config is null");
        readOnly = config.isReadOnly();
        jarDirectory = config.getJarConfigurationDir();

        for (File file : listJarMetaFiles(jarDirectory)) {
            String jarName = Files.getNameWithoutExtension(file.getName());

            FunctionJar functionJar;
            try {
                functionJar = fromFile(file);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading jar meta file " + file, e);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            udfJarMap.put(jarName, functionJar);
        }
    }

    @Override
    public List<FunctionJar> getJars()
    {
        return udfJarMap.values().stream().filter(FunctionJar::notInPluginDir).collect(toImmutableList());
    }

    @Override
    public void addJar(FunctionJar functionJar)
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Function jar store is read only");
        }

        try {
            savaAsFile(functionJar);
        }
        catch (IOException e) {
            log.error(e, "Could not store function jar store");
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not store function jar store");
        }

        udfJarMap.put(functionJar.getJarName(), functionJar);
    }

    @Override
    public void dropJar(String jarUrl, GlobalFunctionCatalog globalFunctionCatalog)
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Function jar store is read only");
        }

        String jarName = getJarNameBasedType(jarUrl);
        try {
            File file = toFile(jarName);
            FunctionJar functionJar = fromFile(file);
            if (null != functionJar) {
                globalFunctionCatalog.dropFunctions(functionJar.getFunctionIds());
            }
            udfJarMap.remove(jarName);
            file.delete();
        }
        catch (Exception e) {
            log.error(e, "Could not drop function jar store");
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not drop function jar store");
        }
    }

    @Override
    public boolean exists(String jarName)
    {
        return udfJarMap.containsKey(jarName);
    }

    private List<File> listJarMetaFiles(File jarDirectory)
    {
        if (jarDirectory == null || !jarDirectory.isDirectory()) {
            return ImmutableList.of();
        }

        File[] files = jarDirectory.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(files)
                .filter(File::isFile)
                .filter(file -> file.getName().endsWith(jarMetaFileExtension))
                .collect(toImmutableList());
    }

    private static FunctionJar fromFile(File file)
            throws IOException, ClassNotFoundException
    {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(file));
        return (FunctionJar) objectInputStream.readObject();
    }

    private void savaAsFile(FunctionJar functionJar)
            throws IOException
    {
        File file = new File(jarDirectory, functionJar.getJarName() + jarMetaFileExtension);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdir();
        }
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        try (FileOutputStream fileInputStream = new FileOutputStream(file)) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileInputStream)) {
                objectOutputStream.writeObject(functionJar);
            }
        }
    }

    private File toFile(String jarName)
    {
        return new File(jarDirectory, jarName + jarMetaFileExtension);
    }
}

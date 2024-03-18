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
package io.trino.plugin.varada.di;

import com.google.inject.Binder;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.ExceptionThrower;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.CoordinatorNativeConnectorSync;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeConnectorSync;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeExceptionThrower;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeLogger;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageEngine;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.varada.storage.read.NativeRangeFillerService;
import io.trino.plugin.varada.storage.read.RangeFillerService;
import io.trino.spi.connector.ConnectorContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Module for binding {@link NativeStorageEngine} to {@link StorageEngine}.
 * Should be used in Production but not necessarily in Testing.
 */
public class VaradaNativeStorageEngineModule
        implements VaradaBaseModule
{
    private static final String SVE_SUFFIX = "_g3";
    private static final boolean SKIP_SVE = true;
    private final boolean isWorker;
    private static final Logger logger = Logger.get(VaradaNativeStorageEngineModule.class);
    private static final String baseNativeLibName = "presto-varada-jni";
    private final boolean isSingle;

    public VaradaNativeStorageEngineModule(ConnectorContext context, Map<String, String> config)
    {
        this.isWorker = VaradaBaseModule.isWorker(context, config);
        this.isSingle = VaradaBaseModule.isSingle(config);
    }

    private static void loadLibrary(Path absoluteLibraryPath)
    {
        try {
            System.load(absoluteLibraryPath.toString());
        }
        catch (Throwable t) {
            logger.error("Error loading: %s", absoluteLibraryPath);
            throw new RuntimeException("Error loading WARP native library", t);
        }
    }

    public static String addSveSuffixIfNeeded()
    {
        if (!SKIP_SVE) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get("/proc/cpuinfo"), Charset.defaultCharset())) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Check if the line starts with "Features" and contains "sve"
                    if (line.startsWith("Features") && line.contains("sve")) {
                        return SVE_SUFFIX;
                    }
                }
            }
            catch (IOException e) {
                logger.debug("Using non-sve library because we got exception %s", e);
            }
        }
        return "";
    }

    public static Path getNativeLibrariesDirectory()
    {
        String currentArchitecture = System.getProperty("os.arch") + addSveSuffixIfNeeded();
        Class<VaradaNativeStorageEngineModule> klass = VaradaNativeStorageEngineModule.class;
        String pluginPath = klass.getProtectionDomain().getCodeSource().getLocation().getPath();
        String pluginDirectory = pluginPath.substring(0, pluginPath.lastIndexOf(File.separator));

        Path archSpecificPath = Path.of(pluginDirectory, currentArchitecture).toAbsolutePath();
        if (Files.exists(archSpecificPath)) {
            logger.info("Loading native Warp-Speed lib from path: %s, readable: %s, executable: %s", archSpecificPath, Files.isReadable(archSpecificPath), Files.isExecutable(archSpecificPath));
            return archSpecificPath;
        }
        logger.warn("Failed to find specific architecture: %s lib. Loading native Warp-Speed without specific architecture",
                currentArchitecture);
        return Path.of(pluginDirectory).toAbsolutePath();
    }

    private static void loadNativeLibrary()
    {
        Path nativeLibrariesDirectoryPath = getNativeLibrariesDirectory();
        loadLibrary(nativeLibrariesDirectoryPath.resolve(System.mapLibraryName(baseNativeLibName)));
    }

    @Override
    public void configure(Binder binder)
    {
        if (isWorker) {
            logger.debug("loading nativeConnectorSync");
            binder.bind(ConnectorSync.class).to(NativeConnectorSync.class);
            binder.bind(RangeFillerService.class).to(NativeRangeFillerService.class);
            binder.bind(StorageEngine.class).toProvider(WorkerStorageEngineProvider.class);
        }
        else {
            logger.debug("loading nativeConnectorSync");
            if (isSingle) {
                binder.bind(ConnectorSync.class).to(NativeConnectorSync.class);
                binder.bind(StorageEngine.class).toProvider(WorkerStorageEngineProvider.class);
                binder.bind(RangeFillerService.class).to(NativeRangeFillerService.class);
            }
            else {
                binder.bind(ConnectorSync.class).to(CoordinatorNativeConnectorSync.class);
                binder.bind(StorageEngine.class).toProvider(CoordinatorStorageEngineProvider.class);
            }
        }
        binder.bind(StorageEngineConstants.class).to(NativeStorageEngineConstants.class);
        binder.bind(NativeLogger.class);
        binder.bind(ExceptionThrower.class).to(NativeExceptionThrower.class).asEagerSingleton();
        binder.bind(NativeStorageStateHandler.class);
    }

    static {
        loadNativeLibrary();
    }
}

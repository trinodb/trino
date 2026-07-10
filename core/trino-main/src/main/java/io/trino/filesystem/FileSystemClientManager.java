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
package io.trino.filesystem;

import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.filesystem.HetuFileSystemClientFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FileSystemClientManager
{
    private static final Logger LOG = Logger.get(FileSystemClientManager.class);
    private static final String FS_CLIENT_TYPE = "fs.client.type";
    private static final String FS_CONFIG_DIR = "etc/filesystem/";
    private static final String DEFAULT_CONFIG_NAME = "default";
    private static final String TEST_HDFS = "__test__hdfs__";

    private static final Map<String, HetuFileSystemClientFactory> fileSystemFactories = new ConcurrentHashMap<>();
    private static final Map<String, Properties> availableFileSystemConfigs = new ConcurrentHashMap<>();

    public FileSystemClientManager()
    {
        // Default filesystem to be a local filesystem client
        Properties defaultProfile = new Properties();
        defaultProfile.setProperty(FS_CLIENT_TYPE, "local");

        Properties testHdfsProfile = new Properties();
        testHdfsProfile.setProperty("fs.client.type", "hdfs");
        testHdfsProfile.setProperty("hdfs.config.resources", "");
        testHdfsProfile.setProperty("hdfs.authentication.type", "NONE");

        availableFileSystemConfigs.put(DEFAULT_CONFIG_NAME, defaultProfile);
        availableFileSystemConfigs.put(TEST_HDFS, testHdfsProfile);
    }

    public void addFileSystemClientFactories(HetuFileSystemClientFactory factory)
    {
        fileSystemFactories.putIfAbsent(factory.getName(), factory);
    }

    /**
     * Loads pre-defined file system profiles in the etc folder.
     * These configs are loaded for clients' usage in presto-main package
     * through {@link FileSystemClientManager#getFileSystemClient(String, Path)}.
     *
     * @throws IOException when exceptions occur during reading profiles
     */
    public void loadFactoryConfigs()
            throws IOException
    {
        LOG.info(String.format("-- Available file system client factories: %s --", fileSystemFactories.keySet().toString()));
        LOG.info("-- Loading file system configs --");

        File configDir = new File(FS_CONFIG_DIR);
        if (!configDir.exists() || !configDir.isDirectory()) {
            LOG.info("-- File system configs not found. Skipped loading --");
            return;
        }

        String[] filesystems = requireNonNull(configDir.list(),
                "Error reading file system config directory: " + FS_CONFIG_DIR);

        for (String fileName : filesystems) {
            if (!fileName.endsWith(".properties")) {
                continue;
            }
            String configName = fileName.replaceAll("\\.properties", "");
            File configFile = new File(FS_CONFIG_DIR + fileName);
            Properties properties = loadProperties(configFile);

            // validate config file: type is specified and corresponding factory is available
            String configType = properties.getProperty(FS_CLIENT_TYPE);
            checkState(configType != null, "%s must be specified in %s",
                    FS_CLIENT_TYPE, configFile.getCanonicalPath());
            checkState(fileSystemFactories.containsKey(configType),
                    "Factory for file system type %s not found", configType);

            // register profile into the map. will overwrite existing profile
            availableFileSystemConfigs.put(configName, properties);
            LOG.info(String.format("Loaded '%s' file system config '%s'", configType, configName));
        }

        LOG.info(String.format("-- Loaded file system profiles: %s --",
                availableFileSystemConfigs.keySet().toString()));
    }

    /**
     * Get the default file system client
     *
     * @return a file system client constructed by default configurations
     * @throws IOException if invalid path
     */
    public HetuFileSystemClient getFileSystemClient(Path root)
            throws IOException
    {
        return getFileSystemClient(DEFAULT_CONFIG_NAME, root);
    }

    /**
     * Get a file system client with pre-defined profile in filesystem config folder
     *
     * @param name name of the filesystem profile defined in the filesystem config folder,
     * or the default profile name (same as {@link FileSystemClientManager#getFileSystemClient(Path)} if provide default name)
     * @param root Workspace root of the filesystem client. It will only be allowed to access filesystem within this directory.
     * @return a {@link HetuFileSystemClient}
     * @throws IOException exception thrown during constructing the client
     */
    public HetuFileSystemClient getFileSystemClient(String name, Path root)
            throws IOException
    {
        if (!availableFileSystemConfigs.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Profile %s is not available. Please check the name provided.", name));
        }
        Properties fsConfig = availableFileSystemConfigs.get(name);
        String type = fsConfig.getProperty(FS_CLIENT_TYPE);
        HetuFileSystemClientFactory factory = fileSystemFactories.get(type);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.getFileSystemClient(fsConfig, root);
        }
    }

    /**
     * Get a file system client with a user-defined properties object
     *
     * @param properties properties used to construct the file system client
     * @param root Workspace root of the filesystem client. It will only be allowed to access filesystem within this directory.
     * @return a {@link HetuFileSystemClient}
     * @throws IOException exception thrown during constructing the client
     */
    public HetuFileSystemClient getFileSystemClient(Properties properties, Path root)
            throws IOException
    {
        String type = checkProperty(properties, FS_CLIENT_TYPE);
        checkState(fileSystemFactories.containsKey(type),
                "Factory for file system type %s not found", type);
        HetuFileSystemClientFactory factory = fileSystemFactories.get(type);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.getFileSystemClient(properties, root);
        }
    }

    private Properties loadProperties(File configFile)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            properties.load(in);
        }
        return properties;
    }

    private String checkProperty(Properties properties, String key)
    {
        String val = properties.getProperty(key);
        if (val == null) {
            throw new IllegalArgumentException(String.format("Configuration entry '%s' must be specified", key));
        }
        return val;
    }

    /**
     * Utility method to evaluate if a filesystem profile can be used as a shared filesystem (e.g. hdfs).
     * <p>
     * Filesystems which are not shared may not work for distributed tasks across the cluster if it has more than 1 nodes.
     *
     * @param name name of the filesystem profile defined in the filesystem config folder,
     * or the default profile name (same as {@link FileSystemClientManager#getFileSystemClient(Path)} if provide default name)
     * @return if the filesystem client linked to this name can be used as a shared filesystem across the cluster
     */
    public boolean isFileSystemLocal(String name)
    {
        if (!availableFileSystemConfigs.containsKey(name)) {
            throw new IllegalArgumentException(String.format("Profile %s is not available. Please check the name provided.", name));
        }
        Properties fsConfig = availableFileSystemConfigs.get(name);
        return fsConfig.getProperty(FS_CLIENT_TYPE).equals("local");
    }
}

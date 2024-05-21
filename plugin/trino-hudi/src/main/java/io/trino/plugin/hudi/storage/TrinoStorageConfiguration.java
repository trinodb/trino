package io.trino.plugin.hudi.storage;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hudi.io.HudiTrinoIOFactory;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS;
import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;

public class TrinoStorageConfiguration extends StorageConfiguration {
    private final Map<String, String> configMap;

    public TrinoStorageConfiguration() {
        this(getDefaultConfigs());
    }

    public TrinoStorageConfiguration(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    public static Map<String, String> getDefaultConfigs() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(HOODIE_IO_FACTORY_CLASS.key(), HudiTrinoIOFactory.class.getName());
        configMap.put(HOODIE_STORAGE_CLASS.key(), HudiTrinoStorage.class.getName());
        return configMap;
    }

    @Override
    public StorageConfiguration newInstance() {
        return new TrinoStorageConfiguration(new HashMap<>(configMap));
    }

    @Override
    public Object unwrap() {
        return configMap;
    }

    @Override
    public Object unwrapCopy() {
        return new HashMap<>(configMap);
    }

    @Override
    public void set(String key, String value) {
        configMap.put(key, value);
    }

    @Override
    public Option<String> getString(String key) {
        return Option.ofNullable(configMap.get(key));
    }

    @Override
    public StorageConfiguration getInline() {
        return newInstance();
    }
}

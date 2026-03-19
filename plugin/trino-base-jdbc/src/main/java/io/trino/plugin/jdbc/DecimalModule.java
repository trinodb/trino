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
package io.trino.plugin.jdbc;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.util.Objects.requireNonNull;

// TODO this class should eventually be removed
public class DecimalModule
        implements Module
{
    private final MappingToNumber mappingToNumber;

    /**
     * Creates a module that installs decimal configs with user-visible deprecation.
     * This should be used when connector wants to migrate away from the decimal configs,
     * most likely towards the new high-precision decimal type.
     */
    public static DecimalModule withNumberMapping(MappingToNumber mappingToNumber)
    {
        return new DecimalModule(mappingToNumber);
    }

    /**
     * Creates a module that installs decimal configs without user-visible deprecation.
     * This constructor is deprecated, connectors should migrate to {@link #withNumberMapping}
     * and eventually stop using this class.
     */
    @Deprecated
    public DecimalModule()
    {
        this(MappingToNumber.UNSUPPORTED);
    }

    private DecimalModule(MappingToNumber mappingToNumber)
    {
        this.mappingToNumber = requireNonNull(mappingToNumber, "mappingToNumber is null");
    }

    @Override
    public void configure(Binder binder)
    {
        boolean supportMapToNumber;
        switch (mappingToNumber) {
            case UNSUPPORTED -> {
                supportMapToNumber = false;
                configBinder(binder).bindConfig(LegacyDecimalConfig.class);
                binder.bind(DecimalConfig.class).to(LegacyDecimalConfig.class);
            }
            case OFF_BY_DEFAULT -> {
                supportMapToNumber = true;
                configBinder(binder).bindConfig(LegacyDecimalConfig.class);
                binder.bind(DecimalConfig.class).to(LegacyDecimalConfig.class);
            }
            case ON_BY_DEFAULT -> {
                supportMapToNumber = true;
                configBinder(binder).bindConfig(DeprecatedDecimalConfig.class);
                binder.bind(DecimalConfig.class).to(DeprecatedDecimalConfig.class);
            }
            default -> {
                throw new IllegalStateException("Unsupported mappingToNumber value: " + mappingToNumber);
            }
        }

        binder.bind(SupportMapToNumber.class).toInstance(new SupportMapToNumber(supportMapToNumber));
        binder.bind(ValidateMapToNumber.class).asEagerSingleton();
        bindSessionPropertiesProvider(binder, DecimalSessionSessionProperties.class);
    }

    public enum MappingToNumber
    {
        /**
         * Mapping to Trino {@code NUMBER} is not supported. This is the default.
         *
         * @deprecated For transition period only.
         */
        @Deprecated
        UNSUPPORTED,

        /**
         * Mapping to Trino {@code NUMBER} is supported, but not enabled by default.
         * The other decimal mappings are not deprecated.
         *
         * @deprecated For transition period only.
         */
        @Deprecated
        OFF_BY_DEFAULT,

        /**
         * Mapping to Trino {@code NUMBER} is supported and enabled by default.
         * The other decimal mappings are deprecated.
         */
        ON_BY_DEFAULT,
    }

    record SupportMapToNumber(boolean value) {}

    private static class ValidateMapToNumber
    {
        @Inject
        public ValidateMapToNumber(DecimalConfig decimalConfig, SupportMapToNumber supportMapToNumber)
        {
            if (decimalConfig.getDecimalMapping() == DecimalConfig.DecimalMapping.MAP_TO_NUMBER && !supportMapToNumber.value()) {
                throw new IllegalStateException("MAP_TO_NUMBER decimal mapping is not supported in this connector");
            }
        }
    }
}

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
package io.trino.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

public class UserAgentBuilder
{
    private static final Joiner.MapJoiner MAP_JOINER = Joiner.on(" ").withKeyValueSeparator("=");
    private static final String LANGUAGE = "lang/java";
    private static final String OS_NAME = "os";
    private static final String OS_VERSION = "os/version";
    private static final String ARCH = "arch";
    private static final String VENDOR = "java/vendor";
    private static final String VM_NAME = "java/vm";
    private static final String LOCALE = "locale";

    private UserAgentBuilder() {}

    public static String createUserAgent(String product)
    {
        return createUserAgent(product, getProductVersion());
    }

    public static String createUserAgent(String product, String version)
    {
        return createUserAgent(product, version, ImmutableMap.of());
    }

    public static String createUserAgent(String product, Map<String, String> metadata)
    {
        return createUserAgent(product, getProductVersion(), metadata);
    }

    public static String createUserAgent(String product, String version, Map<String, String> metadata)
    {
        ImmutableMap.Builder<String, String> sourceBuilder = ImmutableMap.builder();
        sourceBuilder.put(OS_NAME, sanitize(StandardSystemProperty.OS_NAME.value()));
        sourceBuilder.put(OS_VERSION, sanitize(StandardSystemProperty.OS_VERSION.value()));
        sourceBuilder.put(ARCH, sanitize(StandardSystemProperty.OS_ARCH.value()));
        sourceBuilder.put(LANGUAGE, StandardSystemProperty.JAVA_VM_VERSION.value());
        sourceBuilder.put(VM_NAME, sanitize(StandardSystemProperty.JAVA_VM_NAME.value()));
        sourceBuilder.put(VENDOR, sanitize(StandardSystemProperty.JAVA_VENDOR.value()));
        sourceBuilder.put(LOCALE, Locale.getDefault().toLanguageTag());
        sourceBuilder.putAll(metadata);

        return String.format("%s/%s", product, version) + " " + MAP_JOINER.join(sourceBuilder.buildOrThrow());
    }

    @VisibleForTesting
    static String sanitize(String value)
    {
        return value.replaceAll("[^a-zA-Z0-9_.-]+", "_");
    }

    private static String getProductVersion()
    {
        String version = UserAgentBuilder.class.getPackage().getImplementationVersion();
        return firstNonNull(version, "unknown");
    }
}

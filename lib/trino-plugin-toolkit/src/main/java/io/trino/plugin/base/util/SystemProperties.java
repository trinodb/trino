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
package io.trino.plugin.base.util;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

public final class SystemProperties
{
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private SystemProperties() {}

    public static synchronized void setJavaSecurityKrb5Conf(String value)
    {
        set(JAVA_SECURITY_KRB5_CONF, value);
    }

    public static synchronized void set(String key, String newValue)
    {
        String currentValue = System.getProperty(key);
        checkSameValues(key, newValue, currentValue);
        System.setProperty(key, newValue);
    }

    private static void checkSameValues(String key, String newValue, String currentValue)
    {
        checkState(
                currentValue == null || Objects.equals(currentValue, newValue),
                "Refusing to set system property '%s' to '%s', it is already set to '%s'",
                key,
                newValue,
                currentValue);
    }
}

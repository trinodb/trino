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

import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public enum KerberosUtils
{
    INSTANCE;

    private static final Logger LOG = Logger.get(KerberosUtils.class);
    public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
    public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

    private static final long DEFAULT_TIMEOUT = 30 * 60;
    private ReentrantLock lock = new ReentrantLock(true);

    public boolean tryLock()
    {
        try {
            return lock.tryLock(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            return false;
        }
    }

    public void unlock()
    {
        if (lock.isLocked()) {
            try {
                lock.unlock();
            }
            catch (Exception e) {
            }
        }
    }

    public void refreshKrb5Conf(String hiveMetastoreKrb5)
            throws IOException
    {
        if (null == hiveMetastoreKrb5) {
            LOG.debug("refreshKrb5Conf null");
            return;
        }

        LOG.debug("refreshKrb5Conf file %s", hiveMetastoreKrb5);
        System.setProperty("java.security.krb5.conf", hiveMetastoreKrb5);

        long startTime = System.currentTimeMillis();
        try {
            Class<?> classRef;
            if (IBM_JAVA) {
                classRef = Class.forName("com.ibm.security.krb5.internal.Config");
            }
            else {
                classRef = Class.forName("sun.security.krb5.Config");
            }
            Method getRefreshMethod = classRef.getMethod("refresh");
            getRefreshMethod.setAccessible(true);
            getRefreshMethod.invoke(null);
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e.getMessage());
        }

        long endTime = System.currentTimeMillis();
        LOG.debug("refreshKrb5Conf time cost %s ms", endTime - startTime);
    }

    class Krb5Conf
    {
        private String krb5File;
        private Exception exception;
        private HashMap<String, Object> stanzaTable = new HashMap<>();

        public Krb5Conf(String krb5File)
        {
            this.krb5File = krb5File;
        }

        public boolean init()
        {
            try {
                List<String> list = loadConfigFile(this.krb5File);
                this.stanzaTable = parseStanzaTable(list);
                return true;
            }
            catch (IOException e) {
                this.exception = e;
            }
            return false;
        }

        public String get(String... keys)
        {
            List<String> v = getString0(keys);
            if (v == null) {
                return null;
            }
            return v.get(v.size() - 1);
        }

        public Exception getException()
        {
            return this.exception;
        }

        private List<String> loadConfigFile(final String fileName)
                throws IOException
        {
            try {
                List<String> v = new ArrayList<>();
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))) {
                    String line;
                    String previous = null;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.startsWith("#") || line.isEmpty()) {
                            // ignore comments and blank line
                            // Comments start with #.
                            continue;
                        }

                        if (line.startsWith("[")) {
                            if (!line.endsWith("]")) {
                                throw new IOException("Illegal config content:" + line);
                            }
                            if (previous != null) {
                                v.add(previous);
                                v.add("}");
                            }
                            String title = line.substring(1, line.length() - 1).trim();
                            if (title.isEmpty()) {
                                throw new IOException("Illegal config content:" + line);
                            }
                            previous = title + " = {";
                        }
                        else if (line.startsWith("{")) {
                            if (previous == null) {
                                throw new IOException("Config file should not start with \"{\"");
                            }
                            previous += " {";
                            if (line.length() > 1) {
                                // { and content on the same line
                                v.add(previous);
                                previous = line.substring(1).trim();
                            }
                        }
                        else {
                            if (previous == null) {
                                throw new IOException("Config file must starts with a section");
                            }
                            v.add(previous);
                            previous = line;
                        }
                    }
                    if (previous != null) {
                        v.add(previous);
                        v.add("}");
                    }
                }
                return v;
            }
            catch (Exception pe) {
                throw pe;
            }
        }

        private HashMap<String, Object> parseStanzaTable(List<String> v)
                throws IOException
        {
            HashMap<String, Object> current = stanzaTable;
            for (String line : v) {
                // There are 3 kinds of lines
                // 1. a = b
                // 2. a = {
                // 3. }
                if (line.equals("}")) {
                    // Go back to parent, see below
                    current = (HashMap<String, Object>) current.remove(" PARENT ");
                    if (current == null) {
                        throw new IOException("Unmatched close brace");
                    }
                }
                else {
                    int pos = line.indexOf('=');
                    if (pos < 0) {
                        throw new IOException("Illegal config content:" + line);
                    }
                    String key = line.substring(0, pos).trim();
                    String value = trimmed(line.substring(pos + 1));
                    if (value.equals("{")) {
                        HashMap<String, Object> subTable;
                        if (current == stanzaTable) {
                            key = key.toLowerCase(Locale.US);
                        }
                        subTable = new HashMap<>();
                        current.put(key, subTable);
                        // A special entry for its parent. Put whitespaces around,
                        // so will never be confused with a normal key
                        subTable.put(" PARENT ", current);
                        current = subTable;
                    }
                    else {
                        List<String> values;
                        if (current.containsKey(key)) {
                            Object obj = current.get(key);
                            // If a key first shows as a section and then a value,
                            // this is illegal. However, we haven't really forbid
                            // first value then section, which the final result
                            // is a section.
                            if (!(obj instanceof List)) {
                                throw new IOException("Key " + key + "used for both value and section");
                            }
                            values = (List<String>) current.get(key);
                        }
                        else {
                            values = new ArrayList<String>();
                            current.put(key, values);
                        }
                        values.add(value);
                    }
                }
            }
            if (current != stanzaTable) {
                throw new IOException("Not closed");
            }
            return current;
        }

        private String trimmed(String s)
        {
            s = s.trim();
            if (s.isEmpty()) {
                return s;
            }

            if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'
                    || s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
                s = s.substring(1, s.length() - 1).trim();
            }
            return s;
        }

        // Returns final string value(s) for given keys.
        @SuppressWarnings("unchecked")
        private List<String> getString0(String... keys)
        {
            try {
                return (List<String>) get0(keys);
            }
            catch (ClassCastException cce) {
                throw new IllegalArgumentException(cce);
            }
        }

        // Internal method. Returns the value for keys, which can be a sub-stanza
        // or final string value(s).
        // The only method (except for toString) that reads stanzaTable directly.
        @SuppressWarnings("unchecked")
        private Object get0(String... keys)
        {
            Object current = stanzaTable;
            try {
                for (String key : keys) {
                    current = ((HashMap<String, Object>) current).get(key);
                    if (current == null) {
                        return null;
                    }
                }
                return current;
            }
            catch (ClassCastException cce) {
                throw new IllegalArgumentException(cce);
            }
        }
    }
}

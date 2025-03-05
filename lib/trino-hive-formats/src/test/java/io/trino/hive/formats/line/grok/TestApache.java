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
package io.trino.hive.formats.line.grok;

import io.trino.hive.formats.line.grok.exception.GrokException;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestApache
{
    @Test
    public void test001_httpd_access()
            throws GrokException, IOException
    {
        Grok g = Grok.create(ResourceManager.PATTERNS, "%{COMMONAPACHELOG}");

        BufferedReader br = Files.newBufferedReader(Path.of(ResourceManager.ACCESS_LOG));
        String line;
        while ((line = br.readLine()) != null) {
            Match gm = g.match(line);
            gm.captures();
            assertThat(gm.toJson()).isNotNull();
            assertThat(gm.toJson()).isNotEqualTo("{\"Error\":\"Error\"}");
        }
        br.close();
    }

    @Test
    public void test002_nasa_httpd_access()
            throws GrokException, IOException
    {
        Grok g = Grok.create(ResourceManager.PATTERNS, "%{COMMONAPACHELOG}");
        BufferedReader br;
        String line;
        File dir = new File(ResourceManager.NASA);
        for (File child : dir.listFiles()) {
            br = Files.newBufferedReader(Path.of(ResourceManager.NASA + child.getName()));
            while ((line = br.readLine()) != null) {
                Match gm = g.match(line);
                gm.captures();
                assertThat(gm.toJson()).isNotNull();
                assertThat(gm.toJson()).isNotEqualTo("{\"Error\":\"Error\"}");
            }
            br.close();
        }
    }
}

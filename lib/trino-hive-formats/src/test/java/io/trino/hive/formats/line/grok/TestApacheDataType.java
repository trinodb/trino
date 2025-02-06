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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestApacheDataType
{
    private final String line = "64.242.88.10 - - [07/Mar/2004:16:45:56 -0800] \"GET /twiki/bin/attach/Main/PostfixCommands HTTP/1.1\" 401 12846";

    static {
        Locale.setDefault(Locale.ENGLISH);
    }

    @Test
    public void test002_httpd_access_semi()
            throws GrokException, IOException, ParseException
    {
        Grok g = Grok.create(ResourceManager.PATTERNS, "%{IPORHOST:clientip} %{USER:ident;boolean} %{USER:auth} \\[%{HTTPDATE:timestamp;date;dd/MMM/yyyy:HH:mm:ss Z}\\] \"(?:%{WORD:verb;string} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion;float})?|%{DATA:rawrequest})\" %{NUMBER:response;int} (?:%{NUMBER:bytes;long}|-)");

        System.out.println(line);
        Match gm = g.match(line);
        gm.captures();

        assertThat(gm.toJson()).isNotEqualTo("{\"Error\":\"Error\"}");

        Map<String, Object> map = gm.toMap();
        assertThat(map.get("timestamp")).isEqualTo(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z").parse("07/Mar/2004:16:45:56 -0800"));
        assertThat(map.get("response")).isEqualTo(Integer.valueOf(401));
        assertThat(map.get("ident")).isEqualTo(Boolean.FALSE);
        assertThat(map.get("httpversion")).isEqualTo(Float.valueOf(1.1f));
        assertThat(map.get("bytes")).isEqualTo(Long.valueOf(12846));
        assertThat(map.get("verb")).isEqualTo("GET");
    }

    @Test
    public void test002_httpd_access_colon()
            throws GrokException, IOException, ParseException
    {
        Grok g = Grok.create(ResourceManager.PATTERNS, "%{IPORHOST:clientip} %{USER:ident:boolean} %{USER:auth} \\[%{HTTPDATE:timestamp:date:dd/MMM/yyyy:HH:mm:ss Z}\\] \"(?:%{WORD:verb:string} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion:float})?|%{DATA:rawrequest})\" %{NUMBER:response:int} (?:%{NUMBER:bytes:long}|-)");

        System.out.println(line);
        Match gm = g.match(line);
        gm.captures();

        assertThat(gm.toJson()).isNotEqualTo("{\"Error\":\"Error\"}");

        Map<String, Object> map = gm.toMap();
        assertThat(map.get("timestamp")).isEqualTo(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z").parse("07/Mar/2004:16:45:56 -0800"));
        assertThat(map.get("response")).isEqualTo(Integer.valueOf(401));
        assertThat(map.get("ident")).isEqualTo(Boolean.FALSE);
        assertThat(map.get("httpversion")).isEqualTo(Float.valueOf(1.1f));
        assertThat(map.get("bytes")).isEqualTo(Long.valueOf(12846));
        assertThat(map.get("verb")).isEqualTo("GET");
    }
}

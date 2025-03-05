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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestCapture
{
    static Grok grok;

    @BeforeAll
    public static void setUp()
            throws GrokException
    {
        grok = Grok.create(ResourceManager.PATTERNS, null);
    }

    @Test
    public void test001_captureMethod()
            throws GrokException
    {
        grok.addPattern("foo", ".*");
        grok.compile("%{foo}");
        Match m = grok.match("Hello World");
        assertThat(grok.getNamedRegex()).isEqualTo("(?<name0>.*)");
        assertThat(m.getSubject()).isEqualTo("Hello World");
        m.captures();
        assertThat(m.toMap().size()).isEqualTo(1);
        assertThat(m.toMap().get("foo")).isEqualTo("Hello World");
        assertThat(m.toMap().toString()).isEqualTo("{foo=Hello World}");
    }

    @Test
    public void test002_captureMethodMulti()
            throws GrokException
    {
        grok.addPattern("foo", ".*");
        grok.addPattern("bar", ".*");
        grok.compile("%{foo} %{bar}");
        Match m = grok.match("Hello World");
        assertThat(grok.getNamedRegex()).isEqualTo("(?<name0>.*) (?<name1>.*)");
        assertThat(m.getSubject()).isEqualTo("Hello World");
        m.captures();
        assertThat(m.toMap().size()).isEqualTo(2);
        assertThat(m.toMap().get("foo")).isEqualTo("Hello");
        assertThat(m.toMap().get("bar")).isEqualTo("World");
        assertThat(m.toMap().toString()).isEqualTo("{foo=Hello, bar=World}");
    }

    @Test
    public void test003_captureMethodNested()
            throws GrokException
    {
        grok.addPattern("foo", "\\w+ %{bar}");
        grok.addPattern("bar", "\\w+");
        grok.compile("%{foo}");
        Match m = grok.match("Hello World");
        assertThat(grok.getNamedRegex()).isEqualTo("(?<name0>\\w+ (?<name1>\\w+))");
        assertThat(m.getSubject()).isEqualTo("Hello World");
        m.captures();
        assertThat(m.toMap().size()).isEqualTo(2);
        assertThat(m.toMap().get("foo")).isEqualTo("Hello World");
        assertThat(m.toMap().get("bar")).isEqualTo("World");
        assertThat(m.toMap().toString()).isEqualTo("{foo=Hello World, bar=World}");
    }

    @Test
    public void test004_captureNestedRecursion()
            throws GrokException
    {
        grok.addPattern("foo", "%{foo}");
        boolean thrown = false;
        /** Must raise `Deep recursion pattern` execption */
        try {
            grok.compile("%{foo}");
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();
    }

    @Test
    public void test005_captureSubName()
            throws GrokException
    {
        String name = "foo";
        String subname = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_abcdef";
        grok.addPattern(name, "\\w+");
        grok.compile("%{" + name + ":" + subname + "}");
        Match m = grok.match("Hello");
        m.captures();
        assertThat(m.toMap().size()).isEqualTo(1);
        assertThat(m.toMap().get(subname).toString()).isEqualTo("Hello");
        assertThat(m.toMap().toString()).isEqualTo("{abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_abcdef=Hello}");
    }

    @Test
    public void test006_captureOnlyNamed()
            throws GrokException
    {
        grok.addPattern("abcdef", "[a-zA-Z]+");
        grok.addPattern("ghijk", "\\d+");
        grok.compile("%{abcdef:abcdef}%{ghijk}", true);
        Match m = grok.match("abcdef12345");
        m.captures();
        assertThat(m.toMap().size()).isEqualTo(1);
        assertThat(m.toMap().get("ghijk")).isNull();
        assertThat(m.toMap().get("abcdef")).isEqualTo("abcdef");
    }
}

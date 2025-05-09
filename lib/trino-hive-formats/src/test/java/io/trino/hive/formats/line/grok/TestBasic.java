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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestBasic
{
    @Test
    public void test001_compileFailOnInvalidExpression()
            throws GrokException
    {
        Grok g = Grok.create(null);

        List<String> badRegxp = new ArrayList<String>();
        badRegxp.add("[");
        badRegxp.add("[foo");
        badRegxp.add("?");
        badRegxp.add("foo????");
        badRegxp.add("(?-");

        boolean thrown = false;

        // This should always throw
        for (String regx : badRegxp) {
            try {
                g.compile(regx);
            }
            catch (PatternSyntaxException e) {
                thrown = true;
            }
            assertThat(thrown).isTrue();
            thrown = false;
        }
    }

    @Test
    public void test002_compileSuccessValidExpression()
            throws GrokException
    {
        Grok g = Grok.create();

        List<String> regxp = new ArrayList<String>();
        regxp.add("[hello]");
        regxp.add("(test)");
        regxp.add("(?:hello)");
        regxp.add("(?=testing)");

        for (String regx : regxp) {
            g.compile(regx);
        }
    }

    @Test
    public void test003_samePattern()
            throws GrokException
    {
        Grok g = Grok.create();

        String pattern = "Hello World";
        g.compile(pattern);
        assertThat(pattern).isEqualTo(g.getOriginalGrokPattern());
    }

    @Test
    public void test004_sameExpandedPattern()
            throws GrokException
    {
        Grok g = Grok.create();

        g.addPattern("test", "hello world");
        g.compile("%{test}");
        assertThat(g.getNamedRegex()).isEqualTo("(?<name0>hello world)");
    }
}

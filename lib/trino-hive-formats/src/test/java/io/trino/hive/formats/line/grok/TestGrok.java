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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Note: this code is forked from oi.thekraken.grok.api
// Copyright 2014 Anthony Corbacho, and contributors.
public class TestGrok
{
    /**
     * Do some basic test
     *
     * @throws Throwable throws a throwable
     */
    /*
     * public void testGrok() throws Throwable { Grok g = new Grok();
     *
     * g.addPatternFromFile("patterns/base"); g.compile("%{APACHE}"); Match gm =
     * g.match("127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326"
     * ); //Match gm = g.match("10.192.1.47"); gm.captures(); //See the result gm.toJson()
     *
     * }
     */

    Grok g = Grok.create(null);

    public TestGrok()
            throws GrokException
    {
    }

    @Test
    public void test000_basic()
    {
        Grok g = new Grok();
        boolean thrown = false;

        // expected exception
        try {
            g.addPatternFromFile("/good/luck");
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();

        thrown = false;

        try {
            g.addPattern(null, "");
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();

        thrown = false;
        try {
            g.copyPatterns(null);
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();

        thrown = false;
        try {
            g.copyPatterns(new HashMap<String, String>());
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();
    }

    @Test
    public void test000_dummy()
    {
        boolean thrown = false;
        /** This check if grok throw */
        try {
            g.compile(null);
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();
        thrown = false;
        try {
            g.compile("");
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();
        thrown = false;
        try {
            g.compile("      ");
        }
        catch (GrokException e) {
            thrown = true;
        }
        assertThat(thrown).isTrue();
    }

    @Test
    public void test001_static_metod_factory()
            throws Throwable
    {
        Grok staticGrok = Grok.create("%{USERNAME}");
        Match gm = staticGrok.match("root");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=root}");

        gm = staticGrok.match("r00t");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=r00t}");

        gm = staticGrok.match("guest");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=guest}");

        gm = staticGrok.match("guest1234");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=guest1234}");

        gm = staticGrok.match("john doe");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=john}");
    }

    @Test
    public void test001_username()
            throws Throwable
    {
        g.compile("%{USERNAME}");

        Match gm = g.match("root");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=root}");

        gm = g.match("r00t");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=r00t}");

        gm = g.match("guest");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=guest}");

        gm = g.match("guest1234");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=guest1234}");

        gm = g.match("john doe");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USERNAME=john}");
    }

    @Test
    public void test001_username2()
            throws Throwable
    {
        g.compile("%{USER}");

        Match gm = g.match("root");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USER=root}");

        gm = g.match("r00t");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USER=r00t}");

        gm = g.match("guest");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USER=guest}");

        gm = g.match("guest1234");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USER=guest1234}");

        gm = g.match("john doe");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{USER=john}");
    }

    @Test
    public void test002_numbers()
            throws Throwable
    {
        g.compile("%{NUMBER}");

        Match gm = g.match("-42");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{NUMBER=-42}");
    }

    @Test
    public void test003_word()
            throws Throwable
    {
        g.compile("%{WORD}");

        Match gm = g.match("a");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{WORD=a}");

        gm = g.match("abc");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{WORD=abc}");
    }

    @Test
    public void test004_SPACE()
            throws Throwable
    {
        g.compile("%{SPACE}");

        Match gm = g.match("abc dc");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{SPACE=}");
    }

    @Test
    public void test004_number()
            throws Throwable
    {
        g.compile("%{NUMBER}");

        Match gm = g.match("Something costs $55.4!");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{NUMBER=55.4}");
    }

    @Test
    public void test005_NOTSPACE()
            throws Throwable
    {
        g.compile("%{NOTSPACE}");

        Match gm = g.match("abc dc");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{NOTSPACE=abc}");
    }

    @Test
    public void test006_QUOTEDSTRING()
            throws Throwable
    {
        g.compile("%{QUOTEDSTRING:text}");

        Match gm = g.match("\"abc dc\"");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{text=abc dc}");
    }

    @Test
    public void test007_UUID()
            throws Throwable
    {
        g.compile("%{UUID}");

        Match gm = g.match("61243740-4786-11e3-86a7-0002a5d5c51b");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{UUID=61243740-4786-11e3-86a7-0002a5d5c51b}");

        gm = g.match("7F8C7CB0-4786-11E3-8F96-0800200C9A66");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{UUID=7F8C7CB0-4786-11E3-8F96-0800200C9A66}");

        gm = g.match("03A8413C-F604-4D21-8F4D-24B19D98B5A7");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{UUID=03A8413C-F604-4D21-8F4D-24B19D98B5A7}");
    }

    @Test
    public void test008_MAC()
            throws Throwable
    {
        g.compile("%{MAC}");

        Match gm = g.match("5E:FF:56:A2:AF:15");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{MAC=5E:FF:56:A2:AF:15}");
    }

    @Test
    public void test009_IPORHOST()
            throws Throwable
    {
        g.compile("%{IPORHOST}");

        Match gm = g.match("www.google.fr");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{IPORHOST=www.google.fr}");

        gm = g.match("www.google.com");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{IPORHOST=www.google.com}");
    }

    @Test
    public void test010_HOSTPORT()
            throws Throwable
    {
        g.compile("%{HOSTPORT}");

        Match gm = g.match("www.google.fr:80");
        gm.captures();
        assertThat(gm.toMap().toString()).isEqualTo("{HOSTPORT=www.google.fr:80, IPORHOST=www.google.fr, PORT=80}");
    }

    @Test
    public void test011_COMBINEDAPACHELOG()
            throws Throwable
    {
        g.compile("%{COMBINEDAPACHELOG}");
        g.setStrictMode(true);

        Match gm =
                g.match("112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        gm.captures();
        assertThat(gm.toJson()).isNotNull();
        assertThat(gm.toMap().get("agent")).isEqualTo("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22");
        assertThat(gm.toMap().get("clientip")).isEqualTo("112.169.19.192");
        assertThat(gm.toMap().get("httpversion")).isEqualTo("1.1");
        assertThat(gm.toMap().get("timestamp")).isEqualTo("06/Mar/2013:01:36:30 +0900");
        assertThat(gm.toMap().get("TIME")).isEqualTo("01:36:30");

        gm = g.match("112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET /wp-content/plugins/easy-table/themes/default/style.css?ver=1.0 HTTP/1.1\" 304 - \"http://www.nflabs.com/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"");
        gm.captures();
        assertThat(gm.toJson()).isNotNull();
        assertThat(gm.toMap().get("agent")).isEqualTo("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22");
        assertThat(gm.toMap().get("clientip")).isEqualTo("112.169.19.192");
        assertThat(gm.toMap().get("httpversion")).isEqualTo("1.1");
        assertThat(gm.toMap().get("request")).isEqualTo("/wp-content/plugins/easy-table/themes/default/style.css?ver=1.0");
        assertThat(gm.toMap().get("TIME")).isEqualTo("01:36:30");
    }

    /**
     * FROM HERE WE WILL USE STATIC GROK
     */

    @Test
    public void test012_day()
            throws Throwable
    {
        Grok grok = Grok.create("%{DAY}");

        List<String> days = new ArrayList<String>();
        days.add("Mon");
        days.add("Monday");
        days.add("Tue");
        days.add("Tuesday");
        days.add("Wed");
        days.add("Wednesday");
        days.add("Thu");
        days.add("Thursday");
        days.add("Fri");
        days.add("Friday");
        days.add("Sat");
        days.add("Saturday");
        days.add("Sun");
        days.add("Sunday");

        int i = 0;
        for (String day : days) {
            Match m = grok.match(day);
            m.captures();
            assertThat(m.toMap()).isNotNull();
            assertThat(m.toMap().get("DAY")).isEqualTo(days.get(i));
            i++;
        }
    }

    @Test
    public void test013_IpSet()
            throws Throwable
    {
        Grok grok = Grok.create("%{IP}");

        BufferedReader br = Files.newBufferedReader(Path.of(ResourceManager.IP));
        String line;
        while ((line = br.readLine()) != null) {
            Match gm = grok.match(line);
            gm.captures();
            assertThat(gm.toJson()).isNotNull();
            assertThat(gm.toJson()).isNotEqualTo("{\"Error\":\"Error\"}");
            assertThat(gm.toMap().get("IP")).isEqualTo(line);
        }
    }

    @Test
    public void test014_month()
            throws Throwable
    {
        Grok grok = Grok.create("%{MONTH}");

        String[] array = {"Jan", "January", "Feb", "February", "Mar", "March", "Apr", "April", "May", "Jun", "June",
                "Jul", "July", "Aug", "August", "Sep", "September", "Oct", "October", "Nov",
                "November", "Dec", "December"};
        List<String> months = new ArrayList<String>(Arrays.asList(array));
        int i = 0;
        for (String month : months) {
            Match m = grok.match(month);
            m.captures();
            assertThat(m.toMap()).isNotNull();
            assertThat(m.toMap().get("MONTH")).isEqualTo(months.get(i));
            i++;
        }
    }

    @Test
    public void test015_iso8601()
            throws GrokException
    {
        Grok grok = Grok.create("%{TIMESTAMP_ISO8601}");

        String[] array = {
                "2001-01-01T00:00:00",
                "1974-03-02T04:09:09",
                "2010-05-03T08:18:18+00:00",
                "2004-07-04T12:27:27-00:00",
                "2001-09-05T16:36:36+0000",
                "2001-11-06T20:45:45-0000",
                "2001-12-07T23:54:54Z",
                "2001-01-01T00:00:00.123456",
                "1974-03-02T04:09:09.123456",
                "2010-05-03T08:18:18.123456+00:00",
                "2004-07-04T12:27:27.123456-00:00",
                "2001-09-05T16:36:36.123456+0000",
                "2001-11-06T20:45:45.123456-0000",
                "2001-12-07T23:54:54.123456Z"
        };

        List<String> times = new ArrayList<String>(Arrays.asList(array));
        int i = 0;
        for (String time : times) {
            Match m = grok.match(time);
            m.captures();
            assertThat(m.toMap()).isNotNull();
            assertThat(m.toMap().get("TIMESTAMP_ISO8601")).isEqualTo(times.get(i));
            i++;
        }
    }

    @Test
    public void test016_uri()
            throws GrokException
    {
        Grok grok = Grok.create("%{URI}");

        String[] array = {
                "http://www.google.com",
                "telnet://helloworld",
                "http://www.example.com/",
                "http://www.example.com/test.html",
                "http://www.example.com/test.html?foo=bar",
                "http://www.example.com/test.html?foo=bar&fizzle=baz",
                "http://www.example.com:80/test.html?foo=bar&fizzle=baz",
                "https://www.example.com:443/test.html?foo=bar&fizzle=baz",
                "https://user@www.example.com:443/test.html?foo=bar&fizzle=baz",
                "https://user:pass@somehost/fetch.pl",
                "puppet:///",
                "http://www.foo.com",
                "http://www.foo.com/",
                "http://www.foo.com/?testing",
                "http://www.foo.com/?one=two",
                "http://www.foo.com/?one=two&foo=bar",
                "foo://somehost.com:12345",
                "foo://user@somehost.com:12345",
                "foo://user@somehost.com:12345/",
                "foo://user@somehost.com:12345/foo.bar/baz/fizz",
                "foo://user@somehost.com:12345/foo.bar/baz/fizz?test",
                "foo://user@somehost.com:12345/foo.bar/baz/fizz?test=1&sink&foo=4",
                "http://www.google.com/search?hl=en&source=hp&q=hello+world+%5E%40%23%24&btnG=Google+Search",
                "http://www.freebsd.org/cgi/url.cgi?ports/sysutils/grok/pkg-descr",
                "http://www.google.com/search?q=CAPTCHA+ssh&start=0&ie=utf-8&oe=utf-8&client=firefox-a&rls=org.mozilla:en-US:official",
                "svn+ssh://somehost:12345/testing"
        };

        List<String> uris = new ArrayList<String>(Arrays.asList(array));
        int i = 0;
        for (String uri : uris) {
            Match m = grok.match(uri);
            m.captures();
            assertThat(m.toMap()).isNotNull();
            assertThat(m.toMap().get("URI")).isEqualTo(uris.get(i));
            assertThat(m.toMap().get("URIPROTO")).isNotNull();
            i++;
        }
    }

    @Test
    public void test017_nonMachingList()
            throws GrokException
    {
        Grok grok = Grok.create("%{URI}");

        String[] array = {
                "http://www.google.com",
                "telnet://helloworld",
                "",
                "svn+ssh://somehost:12345/testing"
        };
        List<String> uris = new ArrayList<String>(Arrays.asList(array));
        int i = 0;
        for (String uri : uris) {
            Match m = grok.match(uri);
            try {
                m.captures();
            }
            catch (GrokException e) {
                // expected to throw Grok Exception for empty string
                // catching and moving on to rest of test
            }
            assertThat(m.toMap()).isNotNull();
            if (i == 2) {
                assertThat(m.toMap()).isEqualTo(Collections.emptyMap());
            }
            i++;
        }
        assertThat(i).isEqualTo(4);
    }
}

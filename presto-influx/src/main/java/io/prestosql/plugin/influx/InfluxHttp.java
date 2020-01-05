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
package io.prestosql.plugin.influx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;

/* Using raw HTTP because the influx java library has dependency conflicts and puts the burden of quoting identifiers on the caller */
public class InfluxHttp
{
    private final String host;
    private final int port;
    private final boolean useHttps;
    private final String database;
    private final String username;
    private final String password;

    public InfluxHttp(String host, int port, boolean useHttps, String database, String username, String password)
    {
        this.host = host;
        this.port = port;
        this.useHttps = useHttps;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public void execute(String ddl)
    {
        try {
            URL url = getUrl("query",
                    new ImmutableMap.Builder<String, String>()
                            .put("q", ddl)
                            .build());
            checkSuccess((HttpURLConnection) url.openConnection());
        }
        catch (Throwable t) {
            InfluxError.EXTERNAL.fail(t);
        }
    }

    public JsonNode query(String query)
    {
        final JsonNode response;
        try {
            response = new ObjectMapper().readTree(getUrl("query",
                    new ImmutableMap.Builder<String, String>()
                            .put("db", database)
                            .put("q", query)
                            .build()));
        }
        catch (Throwable t) {
            InfluxError.EXTERNAL.fail(t);
            return null;
        }
        JsonNode results = response.get("results");
        InfluxError.GENERAL.check(results != null && results.size() == 1, "expecting one result", query);
        JsonNode result = results.get(0);
        if (result.has("error")) {
            InfluxError.GENERAL.fail(result.get("error").asText(), query);
        }
        return result.get("series");
    }

    public void write(String retentionPolicy, String... lines)
    {
        byte[] body = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
        try {
            URL url = getUrl("write",
                    new ImmutableMap.Builder<String, String>()
                            .put("db", database)
                            .put("rp", retentionPolicy)
                            .build());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode((long) body.length);
            connection.connect();
            try (OutputStream out = connection.getOutputStream()) {
                out.write(body);
            }
            checkSuccess(connection);
        }
        catch (Throwable t) {
            InfluxError.EXTERNAL.fail(t);
        }
    }

    private URL getUrl(String path, Map<String, String> parameters)
            throws UnsupportedEncodingException, MalformedURLException
    {
        String authentication = username != null ?
                username + (password != null ?
                        ":" + password :
                        "") + "@" :
                "";
        StringBuilder query = new StringBuilder();
        for (Map.Entry<String, String> parameter : parameters.entrySet()) {
            query
                    .append(query.length() == 0 ? '?' : '&')
                    .append(parameter.getKey())
                    .append('=')
                    .append(URLEncoder.encode(parameter.getValue(), StandardCharsets.UTF_8.toString()));
        }
        return new URL((useHttps ? "https://" : "http://")
                + authentication
                + host + ":" + port +
                "/" + path +
                query);
    }

    private void checkSuccess(HttpURLConnection connection)
            throws IOException
    {
        int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode >= 300) {
            String errorMessage = "";
            InputStream err = connection.getErrorStream();
            if (err != null) {
                try (Scanner scanner = new Scanner(err)) {
                    scanner.useDelimiter("\\Z");
                    errorMessage += scanner.next();
                }
            }
            InfluxError.EXTERNAL.fail("error writing to influx (" + responseCode + ")", errorMessage);
        }
    }
}

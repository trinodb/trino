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
package io.trino.plugin.querylog;

import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class QuerylogListenerFactory
        implements EventListenerFactory
{
    private final Logger log = Logger.get(QuerylogListenerFactory.class);

    public static final String QUERYLOG_SEND_CREATED = "trino.querylog.log.created";
    public static final String QUERYLOG_SEND_COMPLETED = "trino.querylog.log.completed";
    public static final String QUERYLOG_SEND_SPLIT = "trino.querylog.log.split";
    public static final String QUERYLOG_CONNECT_INET_ADDRESS = "trino.querylog.connect.inet.address";
    public static final String QUERYLOG_CONNECT_SCHEME = "trino.querylog.connect.scheme";
    public static final String QUERYLOG_CONNECT_PORT = "trino.querylog.connect.port";
    public static final String QUERYLOG_CONNECT_HEADERS_PREFIX = "trino.querylog.connect.headers.";

    @Override
    public String getName()
    {
        return "trino-querylog";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        boolean sendCreated = getBooleanConfig(config, QUERYLOG_SEND_CREATED, false);
        boolean sendCompleted = getBooleanConfig(config, QUERYLOG_SEND_COMPLETED, false);
        boolean sendSplit = getBooleanConfig(config, QUERYLOG_SEND_SPLIT, false);

        Optional<String> scheme = Optional.ofNullable(config.get(QUERYLOG_CONNECT_SCHEME));
        Optional<String> inetAddress = Optional.ofNullable(config.get(QUERYLOG_CONNECT_INET_ADDRESS));
        Optional<String> portConfig = Optional.ofNullable(config.get(QUERYLOG_CONNECT_PORT));

        if (inetAddress.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s must be present", QUERYLOG_CONNECT_INET_ADDRESS));
        }

        if (portConfig.isEmpty()) {
            throw new IllegalArgumentException(String.format("%s must be present", QUERYLOG_CONNECT_PORT));
        }

        int port;
        try {
            port = Integer.parseUnsignedInt(portConfig.get());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("%s must be a positive integer value", QUERYLOG_CONNECT_PORT), e);
        }

        if (scheme.isEmpty() || !List.of("http", "https").contains(scheme.get().toLowerCase(Locale.ENGLISH))) {
            throw new IllegalArgumentException(String.format("%s must be either https or http, but not %s", QUERYLOG_CONNECT_SCHEME, scheme.orElse("empty")));
        }

        Map<String, String> httpHeaders = config.keySet().stream()
                .filter(key -> key.startsWith(QUERYLOG_CONNECT_HEADERS_PREFIX))
                .collect(Collectors.toMap(
                        key -> key.replaceFirst("^" + QUERYLOG_CONNECT_HEADERS_PREFIX, ""),
                        config::get));

        try {
            return new QuerylogListener(inetAddress.get(), port, scheme.get().toLowerCase(Locale.ENGLISH), httpHeaders, sendCreated, sendCompleted, sendSplit);
        }
        catch (UnknownHostException e) {
            throw new IllegalArgumentException(String.format("%s is not a valid hostname or it could not be resolved", QUERYLOG_CONNECT_INET_ADDRESS), e);
        }
    }

    /**
     * Get {@code boolean} parameter value, or return default.
     *
     * @param params       Map of parameters
     * @param paramName    Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private static boolean getBooleanConfig(Map<String, String> params, String paramName, boolean paramDefault)
    {
        String value = params.get(paramName);
        if (value != null) {
            if (value.trim().isEmpty()) {
                return true;
            }
            return Boolean.parseBoolean(value);
        }
        return paramDefault;
    }
}

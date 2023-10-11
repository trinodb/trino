package io.trino.plugin.elasticsearch.client;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

public interface ElasticRestClientConfigurator
{
    void configure(HttpAsyncClientBuilder clientBuilder);
}

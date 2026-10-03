package tech.ydb.trino;

import com.google.inject.Inject;
import io.trino.plugin.base.cache.identity.IdentityCacheMapping;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;

import java.util.Set;

public class YdbMetadataFactory extends DefaultJdbcMetadataFactory {
    private final TimestampTimeZoneDomain timestampTimeZoneDomain;
    private final Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public YdbMetadataFactory(
            JdbcClient jdbcClient,
            TimestampTimeZoneDomain timestampTimeZoneDomain,
            Set<JdbcQueryEventListener> jdbcQueryEventListeners,
            IdentityCacheMapping identityCacheMapping
    ) {
        super(jdbcClient, timestampTimeZoneDomain, jdbcQueryEventListeners, identityCacheMapping);
        this.timestampTimeZoneDomain = timestampTimeZoneDomain;
        this.jdbcQueryEventListeners = jdbcQueryEventListeners;
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient) {
        return new DefaultJdbcMetadata(transactionCachingJdbcClient, timestampTimeZoneDomain, false, jdbcQueryEventListeners);
    }
}

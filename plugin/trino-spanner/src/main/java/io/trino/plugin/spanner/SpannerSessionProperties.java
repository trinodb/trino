package io.trino.plugin.spanner;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

public class SpannerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String WRITE_MODE = "write_mode";
    private final ImmutableList<PropertyMetadata<?>> sessionProperties;

    public SpannerSessionProperties()
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.enumProperty(WRITE_MODE,
                        "On write mode INSERT spanner throws an error on insert with duplicate primary keys," +
                                "on write mode UPSERT spanner updates the record with existing primary key with the new record being",
                        Mode.class,
                        Mode.INSERT,
                        false)).build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    enum Mode
    {
        INSERT, UPSERT
    }
}

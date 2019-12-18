package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InfluxSplit implements ConnectorSplit {

    private final HostAddress address;

    @JsonCreator
    public InfluxSplit(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.address = HostAddress.fromParts(requireNonNull(host, "host is null"), port);
    }

    public InfluxSplit(HostAddress address) {
        this.address = requireNonNull(address, "address is null");
    }

    @JsonProperty
    public String getHost() {
        return address.getHostText();
    }

    @JsonProperty
    public int getPort() {
        return address.getPort();
    }


    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
            .add("host", address)
            .toString();
    }
}

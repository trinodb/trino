package io.trino.plugin.deltalake.metastore.delta_sharing;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.nio.file.Path;
import java.util.Optional;

public class DeltaSharingConfig {
    private Path profilePath;
    private String share;
    private String schema;

    @Config("delta.profile-path")
    @ConfigDescription("Path to Delta Sharing profile file")
    public DeltaSharingConfig setProfilePath(Path profilePath) {
        this.profilePath = profilePath;
        return this;
    }

    public Path getProfilePath() {
        return profilePath;
    }

    @Config("delta.share")
    @ConfigDescription("Name of the Share")
    public DeltaSharingConfig setShare(String share)
    {
        this.share = share;
        return this;
    }

    public String getShare() {
        return share;
    }

    @Config("delta.schema")
    @ConfigDescription("Schema In The Share")
    public DeltaSharingConfig setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getSchema() {
        return schema;
    }
}

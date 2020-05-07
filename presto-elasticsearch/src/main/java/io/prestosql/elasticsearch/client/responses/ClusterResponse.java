package io.prestosql.elasticsearch.client.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ClusterResponse
{
    private final String name;
    private final String cluster_name;
    private final String cluster_info;
    private final ClusterVersion version;
    private final String tagline;


    @JsonCreator
    public ClusterResponse(
            @JsonProperty("name") String name,
            @JsonProperty("cluster_name") String cluster_name,
            @JsonProperty("cluster_info") String cluster_info,
            @JsonProperty("version") ClusterVersion version,
            @JsonProperty("tagline") String tagline)
    {
        this.name = name;
        this.cluster_name = cluster_name;
        this.cluster_info = cluster_info;
        this.version = version;
        this.tagline = tagline;

    }

    public String getClusterVersion() {
        return version.number;
    }

    public static class ClusterVersion
    {
        private final String number;
        private final String build_flavor;
        private final String build_type;
        private final String build_hash;
        private final String build_date;
        private final Boolean build_snapshot;
        private final String lucene_version;
        private final String minimum_wire_compatibility_version;
        private final String minimum_index_compatibility_version;

        @JsonCreator
        public ClusterVersion(
                @JsonProperty("number") String number,
                @JsonProperty("build_flavor") String build_flavor,
                @JsonProperty("build_type") String build_type,
                @JsonProperty("build_hash") String build_hash,
                @JsonProperty("build_date") String build_date,
                @JsonProperty("build_snapshot") Boolean build_snapshot,
                @JsonProperty("lucene_version") String lucene_version,
                @JsonProperty("minimum_wire_compatibility_version") String minimum_wire_compatibility_version,
                @JsonProperty("minimum_index_compatibility_version") String minimum_index_compatibility_version)
        {
            this.number = number;
            this.build_flavor = build_flavor;
            this.build_type = build_type;
            this.build_hash = build_hash;
            this.build_date = build_date;
            this.build_snapshot = build_snapshot;
            this.lucene_version = lucene_version;
            this.minimum_wire_compatibility_version = minimum_wire_compatibility_version;
            this.minimum_index_compatibility_version = minimum_index_compatibility_version;
        }
    }



}

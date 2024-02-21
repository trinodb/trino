/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.GeneratedOperations;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.request.GenerateOperationsRequest;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.starburst.schema.discovery.generation.Dialect.TRINO;

public class Tester
{
    private Tester() {}

    public static void main(String[] args)
            throws Exception
    {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        System.out.print("> ");
        URI uri = URI.create(in.readLine());

        DiscoveryTrinoFileSystem fileSystem = uri.getScheme().equals("s3") ? getS3FileSystem() : Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, Util.parquetDataSourceFactory, Util.orcDataSourceFactory, TRINO);
        Map<String, String> options = new HashMap<>(CsvOptions.standard());
        options.put(GeneralOptions.EXCLUDE_PATTERNS, ".*|*.sql");
        options.put(GeneralOptions.MAX_SAMPLE_TABLES, "500");
        DiscoveredSchema discoveredSchema = controller.guess(new GuessRequest(uri, options)).get();
        GenerateOptions generateOptions = new GenerateOptions("test", 10, true, Optional.empty());
        GeneratedOperations operations = controller.generateOperations(new GenerateOperationsRequest(discoveredSchema, generateOptions));
        System.out.println(String.join("", operations.sql()));
/*
        DiscoveredSchema updateDiscoveredSchema = controller.guess(new GuessRequest(uri, options)).get();
        GeneratedOperations operationDifferences = controller.generateOperationDifferences(new GenerateOperationDifferencesRequest(uri, discoveredSchema.tables(), updateDiscoveredSchema.tables(), options, generateOptions));
        System.out.println(String.join("", operationDifferences.sql()));
*/
    }

    private static DiscoveryTrinoFileSystem getS3FileSystem()
    {
        S3FileSystemFactory s3FileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion("us-east-1")
                        .setStreamingPartSize(DataSize.valueOf("5.5MB")));

        return new DiscoveryTrinoFileSystem(s3FileSystemFactory.create(ConnectorIdentity.ofUser("local-discovery")));
    }
}

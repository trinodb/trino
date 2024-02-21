/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.cli;

import io.starburst.schema.discovery.cli.commands.SchemaDiscoveryCommand;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER;

public class SchemaDiscoveryCli
{
    private SchemaDiscoveryCli() {}

    public static class Debug
    {
        @SuppressWarnings("InfiniteLoopStatement")
        public static void main(String[] args)
                throws IOException
        {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            while (true) {
                System.out.print("> ");
                SchemaDiscoveryCli.main(new String[] {in.readLine()});
            }
        }
    }

    public static void main(String[] args)
    {
        CommandLine commandLine = new CommandLine(new SchemaDiscoveryCommand());
        Map<String, CommandLine.IHelpSectionRenderer> helpMap = new HashMap<>(commandLine.getHelpSectionMap());
        helpMap.put(SECTION_KEY_FOOTER, new OptionsHelp());
        commandLine.setHelpSectionMap(helpMap);
        commandLine.setExecutionExceptionHandler((e, __, ___) -> {
            if (Boolean.getBoolean("DEBUG")) {
                e.printStackTrace();
            }
            else {
                System.err.println(e.getMessage());
            }
            return -1;
        });
        commandLine.execute(args);
    }
}

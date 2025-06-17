/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.config;


import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


public class CommandLineConfig {
    private String configPath;

    public CommandLineConfig(String[] args) {
        ArgumentParser parser = ArgumentParsers
                .newFor("Pixels-Sink")
                .build()
                .defaultHelp(true)
                .description("pixels-sink kafka consumer");

        parser.addArgument("-c", "--config")
                .dest("config")
                .required(false)
                .help("config path");

        try {
            Namespace res = parser.parseArgs(args);
            this.configPath = res.getString("config");

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
    }

    public String getConfigPath() {
        return configPath;
    }

}
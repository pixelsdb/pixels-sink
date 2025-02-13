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
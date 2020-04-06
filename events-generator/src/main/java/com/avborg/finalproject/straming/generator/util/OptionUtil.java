package com.avborg.finalproject.straming.generator.util;

import com.avborg.finalproject.straming.generator.model.Params;
import org.apache.commons.cli.*;

public final class OptionUtil {
    private static final long DEFAULT_COUNT = 20L;
    private static final String COUNT_OPTION = "count";
    private static final String BOT_OPTION = "bot";
    private static final String DELAY_OPTION = "delay";
    private static final String ITER_OPTION = "iter";
    private static final String PATH_OPTION = "path";


    public static Params parseParams(String[] args) {
        Options options = new Options();
        Option optionCount = new Option("c", COUNT_OPTION, true, "count of events");
        options.addOption(optionCount);
        Option optionIsBot = new Option("b", BOT_OPTION, false, "bot events or not");
        options.addOption(optionIsBot);
        Option delayOption = new Option("d", DELAY_OPTION, true, "delay after each event");
        options.addOption(delayOption);
        Option iterOption = new Option("i", ITER_OPTION, true, "count of iterations");
        options.addOption(iterOption);
        Option pathOption = new Option("p", PATH_OPTION, true, "path to the output file");
        options.addOption(pathOption);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("events-generator", options);
            System.exit(1);
        }
        Params params = new Params();

        params.setCount(parseLongWithDefault(cmd, COUNT_OPTION, DEFAULT_COUNT));

        params.setBot(cmd.hasOption(BOT_OPTION));
        params.setPath(cmd.getOptionValue(PATH_OPTION, null));
        params.setDelay(parseLongWithDefault(cmd, DELAY_OPTION, 0L));
        params.setIter(parseLongWithDefault(cmd, ITER_OPTION, 1L));
        return params;
    }

    private static Long parseLongWithDefault(CommandLine cmd, String optionName, Long defaultValue) {
        Long result;
        String count = cmd.getOptionValue(optionName, String.valueOf(defaultValue));
        try {
            result = Long.parseLong(count);
        } catch (Exception e) {
            System.out.println("Error parsing count option, default value is " + defaultValue);
            result = defaultValue;
        }
        return result;
    }
}

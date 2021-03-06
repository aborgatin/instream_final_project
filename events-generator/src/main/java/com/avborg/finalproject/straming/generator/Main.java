package com.avborg.finalproject.straming.generator;

import com.avborg.finalproject.straming.generator.model.Event;
import com.avborg.finalproject.straming.generator.model.Params;
import com.avborg.finalproject.straming.generator.service.EventGenerator;
import com.avborg.finalproject.straming.generator.service.EventGeneratorImpl;
import com.avborg.finalproject.straming.generator.util.OptionUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

import static com.avborg.finalproject.straming.generator.util.FileUtil.getFile;
import static com.avborg.finalproject.straming.generator.util.FileUtil.writeToFile;

public class Main {


    public static void main(String[] args) throws IOException, InterruptedException {
        Params params = OptionUtil.parseParams(args);
        ObjectMapper mapper = new ObjectMapper();
        EventGenerator<Event> eventGenerator = new EventGeneratorImpl(params.isBot());
        File file = getFile(params.getPath());
        for (int iter = 0; iter < params.getIter(); iter++) {
            Thread.sleep(params.getDelay());
            StringBuilder sb = new StringBuilder();
            eventGenerator.generateList(params.getCount())
                    .forEach(event -> {
                        try {
                            sb.append(mapper.writeValueAsString(event)).append("\n");
                        } catch (IOException e) {
                            System.out.println("Error writing event: " + event);
                        }
                    });
            writeToFile(file, sb.toString());
            System.out.println(params.getCount() + " events was added to the file " + file.getAbsolutePath());
        }
    }
}

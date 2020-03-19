package com.avborg.finalproject.straming.generator.util;

import com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public final class FileUtil {
    private static final String DEFAULT_FILE_NAME = "data";

    public static File getFile(String path) {
        File result;
        if (!Strings.isNullOrEmpty(path)) {
            result = new File(path);
            if (result.isDirectory()) {
                result = new File(path + "/" + DEFAULT_FILE_NAME + new Date().getTime() + ".txt");
            }
        } else {
            result = new File(DEFAULT_FILE_NAME + new Date().getTime() + ".txt");
        }

        return result;
    }

    public static void writeToFile(File file, String data) throws IOException {
        Files.write(file.toPath(), data.getBytes(), file.exists() ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
    }
}

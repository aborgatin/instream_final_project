package com.avborg.kafka.connect;

import com.avborg.kafka.connect.schema.UserClickDto;
import com.avborg.kafka.connect.schema.UserClickSchemas;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.avborg.kafka.connect.schema.UserClickSchemaFields.*;

public class DirectorySourceTask extends SourceTask {
    private final static String LINE = "line";
    private final static String FILE_UPDATE = "file_update";
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private DirectoryConfig config;
    private List<Path> pathsToRead;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new DirectoryConfig(props);
        pathsToRead = Arrays.stream(config.getDirPath().split(","))
                .map(Paths::get).collect(Collectors.toList());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(config.getPollFrequency());
        List<SourceRecord> records = new ArrayList<>();
        for (Path path : pathsToRead) {
            try {
                Files.walk(path).filter(path1 -> !Files.isDirectory(path1))
                        .forEach(path1 -> {
                            try {
                                records.addAll(convert(path1));
                            } catch (IOException e) {
                                throw new IllegalArgumentException(e);
                            }
                        });
            } catch (Exception ex) {
                LOGGER.warn("Error while reading file " + path.toString(), ex);
            }
        }
        return records;
    }

    private Collection<? extends SourceRecord> convert(Path path) throws IOException {
        List<SourceRecord> recs = new ArrayList<>();
        Map<String, String> offsetKey = Collections.singletonMap("path", path.toString());
        OffsetStorageReader offsetStorageReader = context.offsetStorageReader();
        Map<String, Object> offsetMap = offsetStorageReader.offset(offsetKey);
        long offset = 0;
        if (offsetMap != null) {
            offset = (Long) offsetMap.get(LINE);
        }
        LOGGER.info("offset for path {} equal {}", path.toString(), offset);
        try (Stream<String> lines = Files.lines(path)) {
            AtomicLong offsetObject = new AtomicLong(offset);
            lines.skip(offset).forEach(line -> {
                try {
                    offsetObject.incrementAndGet();
                    Struct key = new Struct(UserClickSchemas.KEY_SCHEMA);
                    Struct value = new Struct(UserClickSchemas.VALUE_SCHEMA);
                    UserClickDto userClickDto = mapper.readValue(line, UserClickDto.class);
                    key.put(IP, userClickDto.getIp());
                    value.put(IP, userClickDto.getIp());
                    value.put(TYPE, userClickDto.getType());
                    value.put(TIME, userClickDto.getEventTime());
                    value.put(URL, userClickDto.getUrl());
                    recs.add(new SourceRecord(
                            offsetKey,
                            Collections.singletonMap(LINE, offsetObject.longValue()),
                            config.getTopic(),
                            UserClickSchemas.KEY_SCHEMA, key,
                            UserClickSchemas.VALUE_SCHEMA, value));
                } catch (JsonProcessingException e) {
                    LOGGER.warn("Error parse a line number {} from file {}: \n {}", offsetObject.longValue(), path.toString(), line);
                    LOGGER.warn(line, e);
                }
            });
        }
        return recs;
    }

    @Override
    public void stop() {

    }

}

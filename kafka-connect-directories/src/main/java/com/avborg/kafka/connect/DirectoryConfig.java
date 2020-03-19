package com.avborg.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DirectoryConfig extends AbstractConfig {

    public static final String DIR_PATH_KEY = "directory.path";
    public static final String DIR_PATH_DOC = "Path to source directory";
    public static final String POLL_FREQUENCY_KEY = "poll.frequency";
    public static final String POLL_FREQUENCY_DOC = "Interval of polling data from the folder";
    public static final String KAFKA_TOPIC_KEY = "topic";
    public static final String KAFKA_TOPIC_DOC = "Kafka topic to send data t0";


    private final String dirPath;
    private final String topic;
    private final Long pollFrequency;

    public DirectoryConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.dirPath = this.getString(DIR_PATH_KEY);
        this.topic = this.getString(KAFKA_TOPIC_KEY);
        this.pollFrequency = this.getLong(POLL_FREQUENCY_KEY);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(KAFKA_TOPIC_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KAFKA_TOPIC_DOC)
                .define(DIR_PATH_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DIR_PATH_DOC)
                .define(POLL_FREQUENCY_KEY, ConfigDef.Type.LONG, 1000, ConfigDef.Importance.MEDIUM, POLL_FREQUENCY_DOC);
    }

    public String getDirPath() {
        return dirPath;
    }

    public String getTopic() {
        return topic;
    }

    public Long getPollFrequency() {
        return pollFrequency;
    }
}

package com.avborg.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.avborg.kafka.connect.DirectoryConfig.DIR_PATH_KEY;

public class DirectoryFilesSourceConnector extends SourceConnector {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private DirectoryConfig config;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        config = new DirectoryConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> dirList = Arrays.asList(config.getDirPath().split(","));
        int numGroups = Math.max(maxTasks, dirList.size());
        return ConnectorUtils.groupPartitions(dirList, numGroups)
                .stream()
                .map(taskDirs -> {
                    Map<String, String> taskProp = new HashMap<>(config.originalsStrings());
                    taskProp.put(DIR_PATH_KEY, String.join(",", taskDirs));
                    return taskProp;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return DirectoryConfig.config();
    }
}

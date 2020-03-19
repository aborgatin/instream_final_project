package com.avborg.kafka.connect.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import static com.avborg.kafka.connect.schema.UserClickSchemaFields.*;

public class UserClickSchemas {


    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("Key schema")
            .version(1)
            .field(IP, Schema.STRING_SCHEMA)
            .build();

    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("Key schema")
            .version(1)
            .field(IP, Schema.STRING_SCHEMA)
            .field(TYPE, Schema.STRING_SCHEMA)
            .field(TIME, Schema.INT64_SCHEMA)
            .field(URL, Schema.STRING_SCHEMA)
            .build();
}

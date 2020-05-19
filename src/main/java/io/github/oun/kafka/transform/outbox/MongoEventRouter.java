package io.github.oun.kafka.transform.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public class MongoEventRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String OPERATION_FIELD_NAME = "op";
    private static final String AFTER_FIELD_NAME = "after";
    private static final String CREATE = "c";
    private static final String READ = "r";
    private static final String DELETE = "d";
    private static final String RECORD_ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope";
    private static final String FIELD_EVENT_ID = "_id";
    private static final String FIELD_EVENT_TYPE = "type";
    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_AGGREGATE_ID = "aggregateId";
    private static final String FIELD_AGGREGATE_TYPE = "aggregateType";

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoEventRouter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public MongoEventRouter() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public R apply(R r) {
        // Ignoring tombstones
        if (r.value() == null) {
            LOGGER.debug("Tombstone message ignored. Message key: \"{}\"", r.key());
            return null;
        }

        // Ignoring messages which do not adhere to the CDC Envelope, for instance:
        // Heartbeat and Schema Change messages
        if (r.valueSchema() == null ||
                r.valueSchema().name() == null ||
                !r.valueSchema().name().endsWith(RECORD_ENVELOPE_SCHEMA_NAME_SUFFIX)) {
            LOGGER.debug("Message without Debezium CDC Envelope ignored. Message key: \"{}\"", r.key());
            return null;
        }

        Struct debeziumEventValue = requireStruct(r.value(), "Detect Debezium Operation");
        String op = debeziumEventValue.getString(OPERATION_FIELD_NAME);
        String after = debeziumEventValue.getString(AFTER_FIELD_NAME);

        if (op.equals(DELETE)) {
            return null;
        }

        if (op.equals(CREATE) || op.equals(READ)) {
            try {
                final Map event = MAPPER.readValue(after, Map.class);
                Long timestamp = debeziumEventValue.getInt64("ts_ms");

                Object eventId = event.get(FIELD_EVENT_ID);
                Object eventType = event.get(FIELD_EVENT_TYPE);
                Object payload = event.get(FIELD_PAYLOAD);
                Object aggregateId = event.get(FIELD_AGGREGATE_ID);
                Object aggregateType = event.get(FIELD_AGGREGATE_TYPE);

                Headers headers = r.headers();
                headers.add("id", eventId.toString(), Schema.STRING_SCHEMA);

                Schema valueSchema = SchemaBuilder.struct()
                        .name("io.github.oun.outbox.Event")
                        .field(FIELD_EVENT_TYPE, Schema.STRING_SCHEMA)
                        .field(FIELD_PAYLOAD, Schema.STRING_SCHEMA)
                        .build();

                String topic = aggregateType.toString().toLowerCase() + "_events";
                Struct value = new Struct(valueSchema)
                        .put(FIELD_EVENT_TYPE, eventType)
                        .put(FIELD_PAYLOAD, payload);

                LOGGER.debug("Route event {} to topic {}", eventId, topic);
                return r.newRecord(
                        topic,
                        null,
                        Schema.STRING_SCHEMA,
                        aggregateId,
                        valueSchema,
                        value,
                        timestamp,
                        headers
                );
            } catch (IOException e) {
                LOGGER.error("Error processing event", e);
            }
        } else {
            LOGGER.info("Record of unexpected op type {}", op);
        }
        return r;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }
}

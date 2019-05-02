package com.logicdee.kafka.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EventRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String EVENT_ID = "_EventId_";
    public static final String DEFAULT_CLASSID_FIELD_NAME = "__TypeId__";

    private static final Logger logger = LoggerFactory.getLogger(EventRouter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public EventRouter() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public R apply(R record) {
        // Ignoring tombstones just in case
        if (record.value() == null) {
            return record;
        }

        Struct struct = (Struct) record.value();
        String op = struct.getString("op");

        System.out.println(struct);

        // ignoring deletions in the events table
        if (op.equals("d")) {
            return null;
        }
        else if (op.equals("c") || op.equals("r")) {
            try {
                Long timestamp = struct.getInt64("ts_ms");
                String after = struct.getString("after");
                Map mongoEvent = mapper.readValue(after, Map.class);

                logger.debug("Received mongodb event {}", mongoEvent);
                String id = (String) mongoEvent.get("_id");
                String key = (String) mongoEvent.get("aggregateId");
                String type = (String) mongoEvent.get("type");
                String aggregateType = (String) mongoEvent.get("aggregateType");
                String topic = aggregateType + "Events";
                String payloadString = (String) mongoEvent.get("payload");

                Map<String, Object> event = new HashMap<>(mongoEvent);
                event.remove("_id");

                if (payloadString != null && !payloadString.isEmpty()) {
                    Map payload = mapper.readValue(payloadString, Map.class);
                    event.putAll(payload);
                }
                String value = mapper.writeValueAsString(event);

                Headers headers = record.headers();
                headers.addString(EVENT_ID, id);
                headers.addString(DEFAULT_CLASSID_FIELD_NAME, type);
                logger.info("Route message key {} value {} to topic {}", key, value, topic);
                return record.newRecord(
                        topic,
                        null,
                        Schema.STRING_SCHEMA,
                        key,
                        Schema.STRING_SCHEMA,
                        value,
                        record.timestamp(),
                        headers
                );
            } catch (Exception e) {
                logger.error("Error processing event", e);
            }
        }
        else {
            logger.info("Record of unexpected op type: " + record);
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }
}

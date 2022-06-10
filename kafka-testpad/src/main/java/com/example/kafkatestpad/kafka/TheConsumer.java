package com.example.kafkatestpad.kafka;

import com.example.kafkatestpad.avro.PayloadCourse;
import com.example.kafkatestpad.avro.PayloadUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TheConsumer {
    @KafkaListener(
            topics = "${app.kafka.consumer.topic}",
            groupId = "consumer-for-all")
    public void listen(ConsumerRecord<String, Bytes> consumerRecord, Acknowledgment acknowledgment) {
//        log.info("received {} [{}]", consumerRecord.value().getClass().getSimpleName(), consumerRecord.value());
        if (consumerRecord.value() != null && consumerRecord.value().get().length > 0) {
            boolean done = false;
            for (Header header : consumerRecord.headers()) {
                if (header.key().equalsIgnoreCase("type")) {
                    switch (new String(header.value())) {
                        case "PayloadUser": {
                            AvroDeserializer<PayloadUser> deserializer = new AvroDeserializer<>(PayloadUser.class);
                            PayloadUser payloadUser = deserializer.deserialize(null, consumerRecord.value().get());
                            log.info("PAYLOADUSER [{}] ... [{}]", payloadUser, consumerRecord.value().get());
                            done = true;
                            break;
                        }
                        case "PayloadCourse": {
                            AvroDeserializer<PayloadCourse> deserializer = new AvroDeserializer<>(PayloadCourse.class);
                            PayloadCourse payloadCourse = deserializer.deserialize(null, consumerRecord.value().get());
                            log.info("PAYLOADCOURSE [{}] ... [{}]", payloadCourse, consumerRecord.value().get());
                            done = true;
                            break;
                        }
                    }
                }
                if (done) {
                    break;
                }
            }
        }
        acknowledgment.acknowledge();
    }
}

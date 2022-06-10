package com.example.kafkatestpad.kafka;

import com.example.kafkatestpad.avro.PayloadCourse;
import com.example.kafkatestpad.avro.PayloadUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
@Slf4j
public class TheProducer {

    @Value("${app.kafka.producer.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendUser(final PayloadUser payloadUser) {
        ProducerRecord<String,Object> record = new ProducerRecord<>(topic, payloadUser.getName(), payloadUser);
        record.headers().add(new RecordHeader("type", PayloadUser.class.getSimpleName().getBytes()));
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);
        future.addCallback(result -> {
            log.info("Sent message=[{}]", payloadUser);
        }, ex -> {
            log.error("Failed to send message=[{}]", payloadUser);
        });
    }

    public void sendCourse(final PayloadCourse payloadCourse) {
        ProducerRecord<String,Object> record = new ProducerRecord<>(topic, String.valueOf(payloadCourse.getId()), payloadCourse);
        record.headers().add(new RecordHeader("type", PayloadCourse.class.getSimpleName().getBytes()));
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(record);
        future.addCallback(result -> {
            log.info("Sent message=[{}]", payloadCourse);
        }, ex -> {
            log.error("Failed to send message=[{}]", payloadCourse);
        });
    }
}

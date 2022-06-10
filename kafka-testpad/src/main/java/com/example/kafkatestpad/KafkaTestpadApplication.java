package com.example.kafkatestpad;

import com.example.kafkatestpad.avro.PayloadCourse;
import com.example.kafkatestpad.avro.PayloadUser;
import com.example.kafkatestpad.kafka.TheProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@SpringBootApplication
public class KafkaTestpadApplication implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTestpadApplication.class, args);
	}

	@Autowired
	TheProducer theProducer;

	@Override
	public void run(ApplicationArguments args) {
		log.info("Ready to produce messages...");
		for (int cnt = 0; cnt < 100; cnt++) {
			if (ThreadLocalRandom.current().nextInt() % 2 == 0) {
				PayloadUser p = PayloadUser.newBuilder()
						.setName("JD_" + System.currentTimeMillis())
						.setDisplayName("John Doe " + cnt)
						.setTitle("Mr " + cnt)
						.build();
				theProducer.sendUser(p);
			} else {
				PayloadCourse c = PayloadCourse.newBuilder()
						.setId(System.currentTimeMillis())
						.setTitle("The Great Book, Vol " + cnt)
						.setUrl("http://books.com/greatbookv" + cnt)
						.build();
				theProducer.sendCourse(c);
			}
		}
	}
}

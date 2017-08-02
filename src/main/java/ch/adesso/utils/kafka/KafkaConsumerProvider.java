package ch.adesso.utils.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.enterprise.inject.Produces;

import org.apache.kafka.clients.consumer.KafkaConsumer;

@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class KafkaConsumerProvider {

	private KafkaConsumer<String, Object> consumer;

	@PostConstruct
	public void init() {
		this.consumer = createConsumer();
	}

	@Produces
	public KafkaConsumer<String, Object> getConsumer() {
		return consumer;
	}

	public KafkaConsumer<String, Object> createConsumer() {
		return new KafkaConsumer<>(loadProperties());
	}

	public Properties loadProperties() {
		Properties properties = new Properties();
		final InputStream stream = KafkaProducerProvider.class.getResourceAsStream("/kafka-consumer.properties");
		if (stream == null) {
			throw new RuntimeException("No kafka producer properties found !!!");
		}
		try {
			properties.load(stream);
		} catch (final IOException e) {
			throw new RuntimeException("Configuration could not be loaded!");
		}

		return updateProperties(properties);
	}

	protected Properties updateProperties(Properties properties) {
		return properties;
	}
}

/*************
 * cil
 **************/

package alice.dip.kafka;

import alice.dip.application.AliDip2BK;
import alice.dip.core.StatisticsManager;
import alice.dip.configuration.KafkaClientConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.protobuf.InvalidProtocolBufferException;

import alice.dip.kafka.AlicePB.NewStateNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import java.util.List;
import java.util.Properties;

public class EndOfRunKafkaConsumer implements Runnable {
	private final KafkaClientConfiguration configuration;
	private final EndOfRunListener endOfRunListener;

	private final Properties properties;

	private final Logger logger = LoggerFactory.getLogger(EndOfRunKafkaConsumer.class);

	public int NoMess = 0;

	public EndOfRunKafkaConsumer(
		KafkaClientConfiguration configuration,
		EndOfRunListener endOfRunListener
	) {
		this.configuration = configuration;
		this.endOfRunListener = endOfRunListener;

		properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers());
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.groupId());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		Thread t = new Thread(this);
		t.start();
	}

	public void run() {

		try (// creating consumer
			 KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)
		) {
			// Subscribing
			consumer.subscribe(List.of(configuration.topics().endOfRun()));

			while (true) {
				ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, byte[]> consumerRecord : consumerRecords) {
					NoMess = NoMess + 1;

					byte[] rawValue = consumerRecord.value();

					try {
						NewStateNotification info = NewStateNotification.parseFrom(rawValue);
						logger.debug(
							"New Kafka mess eor; partition={} offset={} L={} RUN={} {} ENVID={}",
							consumerRecord.partition(),
							consumerRecord.offset(),
							rawValue.length,
							info.getEnvInfo().getRunNumber(),
							info.getEnvInfo().getState(),
							info.getEnvInfo().getEnvironmentId()
						);

						long time = info.getTimestamp();
						int rno = info.getEnvInfo().getRunNumber();

						endOfRunListener.onEndOfRun(time, rno);
					} catch (InvalidProtocolBufferException e) {
						logger.error("ERROR parsing data into obj", e);
					}
				}
			}
		}
	}
}

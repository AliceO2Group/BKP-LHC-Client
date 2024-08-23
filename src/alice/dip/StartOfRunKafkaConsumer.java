/*************
 * cil
 **************/

package alice.dip;

import alice.dip.configuration.KafkaClientConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.protobuf.InvalidProtocolBufferException;

import alice.dip.AlicePB.NewStateNotification;

import java.time.Duration;
import java.util.Arrays;

import java.util.List;
import java.util.Properties;

public class StartOfRunKafkaConsumer implements Runnable {
	public int NoMess = 0;
	public boolean status = true;
	Properties properties;
	DipMessagesProcessor process;

	private final KafkaClientConfiguration configuration;

	public StartOfRunKafkaConsumer(KafkaClientConfiguration configuration, DipMessagesProcessor process) {
		this.configuration = configuration;
		this.process = process;

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
			 KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
			// Subscribing
			consumer.subscribe(List.of(configuration.topics().startOfRun()));

			while (true) {
				ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, byte[]> record : records) {
					byte[] cucu = record.value();

					NoMess = NoMess + 1;

					try {
						NewStateNotification info = NewStateNotification.parseFrom(cucu);
						AliDip2BK.log(1, "KC_SOR.run",
							"New Kafka mess; partition=" + record.partition() + " offset=" + record.offset() + " L=" + cucu.length
								+ " RUN=" + info.getEnvInfo().getRunNumber() + "  " + info.getEnvInfo().getState() + " ENVID = "
								+ info.getEnvInfo().getEnvironmentId());

						long time = info.getTimestamp();
						int rno = info.getEnvInfo().getRunNumber();

						process.newRunSignal(time, rno);
					} catch (InvalidProtocolBufferException e) {
						AliDip2BK.log(4, "KC_SOR.run", "ERROR pasing data into obj e=" + e);
						status = false;
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
}

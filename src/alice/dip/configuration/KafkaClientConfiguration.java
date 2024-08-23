package alice.dip.configuration;

public record KafkaClientConfiguration(
	String bootstrapServers,
	String groupId,
	KafkaClientTopicsConfiguration topics
) {
	public record KafkaClientTopicsConfiguration(String startOfRun, String endOfRun) {
	}
}

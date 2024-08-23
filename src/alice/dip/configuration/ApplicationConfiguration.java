package alice.dip.configuration;

import java.util.Properties;

public record ApplicationConfiguration(
	LoggingConfiguration logging,
	PersistenceConfiguration persistence,
	DipClientConfiguration dipClient,
	BookkeepingClientConfiguration bookkeepingClient,
	KafkaClientConfiguration kafkaClient,
	SimulationConfiguration simulation
) {
	// Default configuration
	private static final int DEFAULT_DEBUG_LEVEL = 1;

	private static final String DEFAULT_APPLICATION_STATE_PERSISTENCE_DIRECTORY = "STATE/";
	private static final String DEFAULT_HISTORY_DIRECTORY = "HistFiles";

	// Minimum variation in energy required for new values to be logged
	private static final float DEFAULT_LOGGED_ENERGY_DELTA = 5f;
	// Minimum variation in beta star required for new values to be logged
	private static final float DEFAULT_LOGGED_BETA_DELTA = 0.001f;
	// Minimum variation in current required for new values to be logged
	private static final float DEFAULT_LOGGED_CURRENT_DELTA = 5f;

	private static final String DEFAULT_DIP_DNS = "dipnsdev.cern.ch";
	private static final String DEFAULT_BOOKKEEPING_URL = "http://localhost:4000";
	private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
	private static final String DEFAULT_KAFKA_START_OF_RUN_TOPIC = "aliecs.env_state.RUNNING";
	private static final String DEFAULT_KAFKA_END_OF_RUN_TOPIC = "aliecs.env_leave_state.RUNNING";
	private static final String DEFAULT_KAFKA_GROUP_ID = "AliDipTest";

	public static ApplicationConfiguration parseProperties(Properties properties) {
		// Logging

		var debugLevelProperty = properties.getProperty("DEBUG_LEVEL");
		var debugLevel = debugLevelProperty != null ? Integer.parseInt(debugLevelProperty) : DEFAULT_DEBUG_LEVEL;

		var loggingConfiguration = new LoggingConfiguration(debugLevel);

		// Persistence

		var saveParametersHistoryPerRunProperty = properties.getProperty("SAVE_PARAMETERS_HISTORY_PER_RUN", "false");
		saveParametersHistoryPerRunProperty = saveParametersHistoryPerRunProperty.trim();
		var saveParametersHistoryPerRun = saveParametersHistoryPerRunProperty.equalsIgnoreCase("Y")
			|| saveParametersHistoryPerRunProperty.equalsIgnoreCase("YES")
			|| saveParametersHistoryPerRunProperty.equalsIgnoreCase("true");

		var runsHistoryDirectoryProperty = properties.getProperty("KEEP_RUNS_HISTORY_DIRECTORY");
		String runsHistoryDirectory = null;
		if (runsHistoryDirectoryProperty != null) {
			runsHistoryDirectory = runsHistoryDirectoryProperty.trim();
		}

		var fillsHistoryDirectoryProperty = properties.getProperty("KEEP_FILLS_HISTORY_DIRECTORY");
		String fillsHistoryDirectory = null;
		if (fillsHistoryDirectoryProperty != null) {
			fillsHistoryDirectory = fillsHistoryDirectoryProperty.trim();
		}

		var persistenceConfiguration = new PersistenceConfiguration(
			DEFAULT_APPLICATION_STATE_PERSISTENCE_DIRECTORY,
			saveParametersHistoryPerRun,
			runsHistoryDirectory,
			fillsHistoryDirectory,
			DEFAULT_HISTORY_DIRECTORY,
			DEFAULT_LOGGED_ENERGY_DELTA,
			DEFAULT_LOGGED_BETA_DELTA,
			DEFAULT_LOGGED_CURRENT_DELTA
		);

		// DIP Client

		var dnsNode = properties.getProperty("DNSnode", DEFAULT_DIP_DNS);
		var dipParametersFileName = properties.getProperty("DipDataProvidersSubscritionFile");

		var dipClientConfiguration = new DipClientConfiguration(
			dnsNode,
			dipParametersFileName
		);

		// Bookkeeping client

		var bookkeepingURL = properties.getProperty("BookkeepingURL", DEFAULT_BOOKKEEPING_URL);
		var bkpToken = properties.getProperty(("BKP_TOKEN"));

		var bookkeepingClientConfiguration = new BookkeepingClientConfiguration(
			bookkeepingURL,
			bkpToken
		);

		// Kafka client

		var kafkaGroupId = properties.getProperty("KAFKA_group_id", DEFAULT_KAFKA_GROUP_ID);
		var kafkaBootstrapServers = properties.getProperty("bootstrapServers", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
		var kafkaTopicStartOfRun = properties.getProperty("KAFKAtopic_SOR", DEFAULT_KAFKA_START_OF_RUN_TOPIC);
		var kafkaTopicEndOfRun = properties.getProperty("KAFKAtopic_EOR", DEFAULT_KAFKA_END_OF_RUN_TOPIC);

		var kafkaClientConfiguration = new KafkaClientConfiguration(
			kafkaBootstrapServers,
			kafkaGroupId,
			new KafkaClientConfiguration.KafkaClientTopicsConfiguration(
				kafkaTopicStartOfRun,
				kafkaTopicEndOfRun
			)
		);

		// Simulation

		var simulateDipEventsProperty = properties.getProperty("SIMULATE_DIP_EVENTS");
		var simulateDipEvents = false;
		if (simulateDipEventsProperty != null) {
			simulateDipEvents = simulateDipEventsProperty.equalsIgnoreCase("Y")
				|| simulateDipEventsProperty.equalsIgnoreCase("YES")
				|| simulateDipEventsProperty.equalsIgnoreCase("true");
		}

		var simulationConfiguration = new SimulationConfiguration(simulateDipEvents);

		// Full configuration

		return new ApplicationConfiguration(
			loggingConfiguration,
			persistenceConfiguration,
			dipClientConfiguration,
			bookkeepingClientConfiguration,
			kafkaClientConfiguration,
			simulationConfiguration
		);
	}
}

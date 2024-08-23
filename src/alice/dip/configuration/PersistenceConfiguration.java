package alice.dip.configuration;

public record PersistenceConfiguration(
	String applicationStateDirectory,
	boolean saveParametersHistoryPerRun,
	String runsHistoryDirectory,
	String fillsHistoryDirectory,
	String parametersHistoryDirectory,
	// Minimum variation in energy required for new values to be logged
	float minimumLoggedEnergyDelta,
	// Minimum variation in beta star required for new values to be logged
	float minimumLoggedBetaDelta,
	// Minimum variation in current required for new values to be logged
	float minimumLoggedCurrentDelta
) {
}

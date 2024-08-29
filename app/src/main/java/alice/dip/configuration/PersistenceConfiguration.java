package alice.dip.configuration;

import java.nio.file.Path;
import java.util.Optional;

public record PersistenceConfiguration(
	// Root directory of persistence
	Path rootPath,

	Path applicationStatePath,

	Optional<Path> runsHistoryPath,
	Optional<Path> fillsHistoryPath,

	boolean saveParametersHistoryPerRun,
	Path parametersHistoryPath,

	// Minimum variation in energy required for new values to be logged
	float minimumLoggedEnergyDelta,
	// Minimum variation in beta star required for new values to be logged
	float minimumLoggedBetaDelta,
	// Minimum variation in current required for new values to be logged
	float minimumLoggedCurrentDelta
) {
}

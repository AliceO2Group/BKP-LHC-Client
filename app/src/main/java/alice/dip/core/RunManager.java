package alice.dip.core;

import alice.dip.application.AliDip2BK;
import alice.dip.configuration.PersistenceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public class RunManager {
	private final PersistenceConfiguration persistenceConfiguration;

	private final Logger logger = LoggerFactory.getLogger(RunManager.class);

	private OptionalInt lastRunNumber = OptionalInt.empty();
	private final List<RunInfoObj> activeRuns = new ArrayList<>();

	public RunManager(PersistenceConfiguration persistenceConfiguration) {
		this.persistenceConfiguration = persistenceConfiguration;
	}

	public synchronized RunInfoObj handleNewRun(
		long date,
		int runNumber,
		LhcFillView fillAtStart,
		LuminosityView luminosityAtStart,
		AliceMagnetsConfigurationView magnetsConfigurationAtStart
	) {

		var fillLogMessage = fillAtStart != null
			? " with FillNo=" + fillAtStart.fillNumber()
			: " but currentFILL is NULL Perhaps Cosmics Run";

		logger.info("NEW RUN NO={}{}", runNumber, fillLogMessage);

		if (this.hasRunByRunNumber(runNumber)) {
			logger.warn("Duplicate new RUN signal RUN_NUMBER={} IGNORE it", runNumber);
			return null;
		}

		var newRun = new RunInfoObj(
			date,
			runNumber,
			fillAtStart,
			luminosityAtStart,
			magnetsConfigurationAtStart
		);

		// Check if there is the new run is right after the last one
		if (fillAtStart != null) {
			if (this.lastRunNumber.isPresent() && runNumber - lastRunNumber.getAsInt() != 1) {
				StringBuilder missingRunsList = new StringBuilder("<<");
				for (
					int missingRunNumber = (lastRunNumber.getAsInt() + 1);
					missingRunNumber < runNumber;
					missingRunNumber++
				) {
					missingRunsList.append(missingRunNumber).append(" ");
				}
				missingRunsList.append(">>");

				logger.error(
					"LOST RUN No Signal! {} New RUN NO={} Last Run No={}",
					missingRunsList,
					runNumber,
					lastRunNumber
				);
			}

			this.lastRunNumber = OptionalInt.of(runNumber);
		}

		activeRuns.add(newRun);

		return newRun;
	}

	public synchronized void handleRunEnd(
		long date,
		int runNumber,
		LhcFillView fillAtEnd,
		AliceMagnetsConfigurationView magnetsConfigurationAtEnd,
		LuminosityView luminosityAtEnd
	) throws RunNotFoundException {
		RunInfoObj activeRun;
		for (var activeRunIndex = 0; activeRunIndex < activeRuns.size(); activeRunIndex++) {
			activeRun = activeRuns.get(activeRunIndex);

			if (activeRun.RunNo == runNumber) {
				endActiveRun(date, activeRun, fillAtEnd, magnetsConfigurationAtEnd, luminosityAtEnd);
				activeRuns.remove(activeRunIndex);
				return;
			}
		}

		throw new RunNotFoundException("Run is not in the active list");
	}

	public void registerNewEnergy(long time, float energy) {
		for (RunInfoObj run : activeRuns) {
			run.addEnergy(time, energy);
		}
	}

	public void registerNewL3MagnetCurrent(long time, float current) {
		for (RunInfoObj run : activeRuns) {
			run.addL3Current(time, current);
		}
	}

	public void registerNewDipoleCurrent(long time, float current) {
		for (RunInfoObj run : activeRuns) {
			run.addDipoleMagnet(time, current);
		}
	}

	private boolean hasRunByRunNumber(int runNumber) {
		return activeRuns.stream()
			.anyMatch(run -> run.RunNo == runNumber);
	}

	private void endActiveRun(
		long date,
		RunInfoObj run,
		LhcFillView fillAtEnd,
		AliceMagnetsConfigurationView magnetsConfigurationAtEnd,
		LuminosityView luminosityAtEnd
	) {
		run.setEORTime(date);
		run.LHC_info_stop = fillAtEnd;
		run.magnetsConfigurationAtStop = magnetsConfigurationAtEnd;
		run.setLuminosityAtStop(luminosityAtEnd);

		writeRunHistFile(run);

		if (this.persistenceConfiguration.saveParametersHistoryPerRun()) {
			if (!run.energyHistory.isEmpty()) {
				String fn = "Energy_" + run.RunNo + ".txt";
				writeHistoryToFile(fn, run.energyHistory);
			}

			if (!run.l3CurrentHistory.isEmpty()) {
				String fn = "L3magnet_" + run.RunNo + ".txt";
				writeHistoryToFile(fn, run.l3CurrentHistory);
			}
		}

		var activeRunsString = activeRuns.stream()
			.map(activeRun -> String.valueOf(activeRun.RunNo))
			.collect(Collectors.joining(", "));

		logger.info("Correctly closed runNo={} ActiveRuns size={} {}", run.RunNo, activeRuns.size(), activeRunsString);

		if (run.LHC_info_start.fillNumber() != run.LHC_info_stop.fillNumber()) {
			logger.error(
				"RUN={} Started FillNo={} and STOPPED with FillNo={}",
				run.RunNo,
				run.LHC_info_start.fillNumber(),
				run.LHC_info_stop.fillNumber()
			);
		}
	}

	private void writeRunHistFile(RunInfoObj run) {
		persistenceConfiguration.runsHistoryPath().ifPresent((runsHistoryPath) -> {
			var runHistoryPath = runsHistoryPath.resolve("run_" + run.RunNo + ".txt");

			try (
				var writer = Files.newBufferedWriter(
					runHistoryPath,
					StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND
				)
			) {
				writer.write(run.toString());
			} catch (IOException e) {
				logger.error("ERROR writing file={}", runsHistoryPath, e);
			}
		});
	}

	private void writeHistoryToFile(String filename, List<TimestampedFloat> history) {
		var historyPath = persistenceConfiguration.parametersHistoryPath().resolve(filename);

		try (
			var writer = Files.newBufferedWriter(
				historyPath,
				StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND
			)
		) {
			for (var historyItem : history) {
				writer.write(historyItem.time() + "," + historyItem.value() + "\n");
			}
		} catch (IOException e) {
			logger.error("ERROR writing file={}", filename, e);
		}
	}
}

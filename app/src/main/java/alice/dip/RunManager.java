package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public class RunManager {
	private final PersistenceConfiguration persistenceConfiguration;
	private final StatisticsManager statisticsManager;

	private OptionalInt lastRunNumber = OptionalInt.empty();
	private final List<RunInfoObj> activeRuns = new ArrayList<>();

	public RunManager(PersistenceConfiguration persistenceConfiguration, StatisticsManager statisticsManager) {
		this.persistenceConfiguration = persistenceConfiguration;
		this.statisticsManager = statisticsManager;
	}

	public synchronized RunInfoObj handleNewRun(
		long date,
		int runNumber,
		LhcFillView fillAtStart,
		LuminosityView luminosityAtStart,
		AliceMagnetsConfigurationView magnetsConfigurationAtStart
	) {
		statisticsManager.incrementNewRunsCount();

		var fillLogMessage = fillAtStart != null
			? " with FillNo=" + fillAtStart.fillNumber()
			: " but currentFILL is NULL Perhaps Cosmics Run";

		AliDip2BK.log(2, "ProcData.newRunSignal", " NEW RUN NO =" + runNumber + fillLogMessage);

		if (this.hasRunByRunNumber(runNumber)) {
			AliDip2BK.log(6, "ProcData.newRunSignal", " Duplicate new  RUN signal =" + runNumber + " IGNORE it");
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

				AliDip2BK.log(
					7,
					"ProcData.newRunSignal",
					" LOST RUN No Signal! " + missingRunsList + "  New RUN NO =" + runNumber
						+ " Last Run No=" + lastRunNumber
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
	) {
		RunInfoObj activeRun;
		for (var activeRunIndex = 0; activeRunIndex < activeRuns.size(); activeRunIndex++) {
			activeRun = activeRuns.get(activeRunIndex);

			if (activeRun.RunNo == runNumber) {
				endActiveRun(date, activeRun, fillAtEnd, magnetsConfigurationAtEnd, luminosityAtEnd);
				activeRuns.remove(activeRunIndex);
				return;
			}
		}

		AliDip2BK.log(4, "ProcData.EndRun", " ERROR RunNo=" + runNumber + " is not in the ACTIVE LIST ");
		statisticsManager.incrementDuplicatedRunsEndCount();
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
		statisticsManager.incrementEndedRunsCount();

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

		var logModule = "ProcData.EndRun";

		AliDip2BK.log(2, logModule, " Correctly closed  runNo=" + run.RunNo
			+ "  ActiveRuns size=" + activeRuns.size() + " " + activeRunsString);

		if (run.LHC_info_start.fillNumber() != run.LHC_info_stop.fillNumber()) {
			AliDip2BK.log(
				5,
				logModule,
				" !!!! RUN =" + run.RunNo + "  Statred FillNo=" + run.LHC_info_start.fillNumber()
					+ " and STOPED with FillNo=" + run.LHC_info_stop.fillNumber()
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
				AliDip2BK.log(4, "ProcData.writeRunHistFile", " ERROR writing file=" + runsHistoryPath + "   ex=" + e);
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
			AliDip2BK.log(4, "ProcData.writeHistFile", " ERROR writing file=" + filename + "   ex=" + e);
		}
	}
}

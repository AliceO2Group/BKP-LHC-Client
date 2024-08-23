package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

	public Optional<RunInfoObj> getRunByRunNumber(int runNumber) {
		return activeRuns.stream()
			.filter(run -> run.RunNo == runNumber)
			.findFirst();
	}

	public boolean hasRunByRunNumber(int runNumber) {
		return activeRuns.stream()
			.anyMatch(run -> run.RunNo == runNumber);
	}

	public void addRun(RunInfoObj run) {
		activeRuns.add(run);
	}

	public void endRun(int runNumber) {
		Optional<RunInfoObj> deletedRun = Optional.empty();

		for (var activeRunIndex = 0; activeRunIndex < activeRuns.size(); activeRunIndex++) {
			if (activeRuns.get(activeRunIndex).RunNo == runNumber) {
				deletedRun = Optional.of(activeRuns.get(activeRunIndex));
				activeRuns.remove(activeRunIndex);
				break;
			}
		}

		var logModule = "ProcData.EndRun";

		deletedRun.ifPresentOrElse(
			run -> {
				statisticsManager.incrementEndedRunsCount();

				if (persistenceConfiguration.runsHistoryDirectory() != null) writeRunHistFile(run);

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

				AliDip2BK.log(2, logModule, " Correctly closed  runNo=" + run.RunNo
					+ "  ActiveRuns size=" + activeRuns.size() + " " + activeRunsString);

				if (run.LHC_info_start.fillNo != run.LHC_info_stop.fillNo) {
					AliDip2BK.log(5, logModule, " !!!! RUN =" + run.RunNo + "  Statred FillNo=" + run.LHC_info_start.fillNo + " and STOPED with FillNo=" + run.LHC_info_stop.fillNo);
				}
			},
			() -> {
				AliDip2BK.log(4, logModule, " ERROR RunNo=" + runNumber + " is not in the ACTIVE LIST ");
				statisticsManager.incrementDuplicatedRunsEndCount();
			}
		);
	}

	public OptionalInt getLastRunNumber() {
		return lastRunNumber;
	}

	public void setLastRunNumber(int runNumber) {
		lastRunNumber = OptionalInt.of(runNumber);
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

	private void writeRunHistFile(RunInfoObj run) {
		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + persistenceConfiguration.runsHistoryDirectory() + "/run_" + run.RunNo + ".txt";

		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));
			String ans = run.toString();
			writer.write(ans);
			writer.close();
		} catch (IOException e) {

			AliDip2BK.log(4, "ProcData.writeRunHistFile", " ERROR writing file=" + full_file + "   ex=" + e);
		}
	}

	private void writeHistoryToFile(String filename, List<TimestampedFloat> history) {
		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + persistenceConfiguration.parametersHistoryDirectory() + "/" + filename;

		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));

			for (var historyItem: history) {
				writer.write(historyItem.time() + "," + historyItem.value() + "\n");
			}

			writer.close();
		} catch (IOException e) {
			AliDip2BK.log(4, "ProcData.writeHistFile", " ERROR writing file=" + filename + "   ex=" + e);
		}
	}
}

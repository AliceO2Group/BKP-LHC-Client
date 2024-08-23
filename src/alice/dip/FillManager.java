package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.io.*;
import java.util.Date;
import java.util.Optional;

public class FillManager {
	private final PersistenceConfiguration persistenceConfiguration;
	private final BookkeepingClient bookkeepingClient;
	private final StatisticsManager statisticsManager;

	private Optional<LhcInfoObj> currentFill = Optional.empty();

	FillManager(
		PersistenceConfiguration persistenceConfiguration,
		BookkeepingClient bookkeepingClient,
		StatisticsManager statisticsManager
	) {
		this.persistenceConfiguration = persistenceConfiguration;
		this.bookkeepingClient = bookkeepingClient;
		this.statisticsManager = statisticsManager;
	}

	public void loadState() {
		String classPath = getClass().getClassLoader().getResource(".").getPath();
		String savedStatePath = classPath + persistenceConfiguration.applicationStateDirectory() + "/save_fill.jso";

		File savedStateFile = new File(savedStatePath);
		if (!savedStateFile.exists()) {
			AliDip2BK.log(2, "ProcData.loadState", " No Fill State file=" + savedStatePath);
			return;
		}

		try {
			var streamIn = new FileInputStream(savedStatePath);
			var objectinputstream = new ObjectInputStream(streamIn);
			LhcInfoObj savedLhcInfo = (LhcInfoObj) objectinputstream.readObject();
			objectinputstream.close();

			if (savedLhcInfo != null) {
				AliDip2BK.log(3, "ProcData.loadState", " Loaded sate for Fill =" + savedLhcInfo.fillNo);
				currentFill = Optional.of(savedLhcInfo);
			}
		} catch (Exception e) {
			AliDip2BK.log(4, "ProcData.loadState", " ERROR Loaded sate from file=" + savedStatePath);
			e.printStackTrace();
		}
	}

	public Optional<LhcInfoObj> getCurrentFill() {
		return this.currentFill;
	}

	public void setCurrentFill(LhcInfoObj currentFill) {
		this.currentFill = Optional.of(currentFill);
		AliDip2BK.log(2, "ProcData.newFillNo", " **CREATED new FILL no=" + currentFill.fillNo);
		bookkeepingClient.createLhcFill(currentFill);
		statisticsManager.incrementNewFillsCount();
		saveState();
	}

	public void handleFillConfigurationChanged(
		long date,
		int fillNumber,
		String beam1ParticleType,
		String beam2ParticleType,
		String activeInjectionScheme,
		int ip2CollisionsCount,
		int bunchesCount
	) {
		if (currentFill.isPresent()) {
			var fill = currentFill.get();

			if (fill.fillNo == fillNumber) { // the same fill no ;
				if (!activeInjectionScheme.contains("no_value")) {
					boolean modi = fill.verifyAndUpdate(date, activeInjectionScheme, ip2CollisionsCount, bunchesCount);
					if (modi) {
						bookkeepingClient.updateLhcFill(fill);
						saveState();
						AliDip2BK.log(2, "ProcData.newFillNo", " * Update FILL no=" + fillNumber);
					}
				} else {
					AliDip2BK.log(
						4,
						"ProcData.newFillNo",
						" * FILL no=" + fillNumber + " AFS=" + activeInjectionScheme
					);
				}
			} else {
				AliDip2BK.log(
					3,
					"ProcData.newFillNo",
					" Received new FILL no=" + fillNumber + "  BUT is an active FILL =" + fill.fillNo + " Close the " + "old one and created the new one"
				);
				fill.endedTime = (new Date()).getTime();
				if (persistenceConfiguration.fillsHistoryDirectory() != null) {
					writeFillHistFile(fill);
				}
				bookkeepingClient.updateLhcFill(fill);

				setCurrentFill(new LhcInfoObj(
					persistenceConfiguration,
					date,
					fillNumber,
					beam1ParticleType,
					beam2ParticleType,
					activeInjectionScheme,
					ip2CollisionsCount,
					bunchesCount
				));
			}
		} else {
			setCurrentFill(new LhcInfoObj(
				persistenceConfiguration,
				date,
				fillNumber,
				beam1ParticleType,
				beam2ParticleType,
				activeInjectionScheme,
				ip2CollisionsCount,
				bunchesCount
			));
		}
	}

	public void setBeamMode(long date, String beamMode) {
		currentFill.ifPresentOrElse(fill -> {
			fill.setBeamMode(date, beamMode);
			AliDip2BK.log(2, "ProcData.newBeamMode", "New beam mode=" + beamMode + "  for FILL_NO=" + fill.fillNo);
			bookkeepingClient.updateLhcFill(fill);
			saveState();
		}, () -> AliDip2BK.log(4, "ProcData.newBeamMode", " ERROR new beam mode=" + beamMode + " NO FILL NO for it"));
	}

	public void setSafeMode(long time, boolean isBeam1, boolean isBeam2) {
		currentFill.ifPresent(fill -> {
			String currentBeamMode = fill.getBeamMode();

			if (currentBeamMode.contentEquals("STABLE BEAMS")) {
				if (!isBeam1 || !isBeam2) {
					fill.setBeamMode(time, "LOST BEAMS");
					AliDip2BK.log(5, "ProcData.newSafeBeams", " CHANGE BEAM MODE TO LOST BEAMS !!! ");
				}
				return;
			}

			if (currentBeamMode.contentEquals("LOST BEAMS")) {
				if (isBeam1 && isBeam2) {
					fill.setBeamMode(time, "STABLE BEAMS");
					AliDip2BK.log(5, "ProcData.newSafeBeams", " RECOVER FROM BEAM LOST TO STABLE BEAMS ");
				}
			}
		});
	}

	public void setBetaStar(long date, float betaStar) {
		this.currentFill.ifPresent(fill -> fill.setLHCBetaStar(date, betaStar));
	}

	/**
	 * Set the current fill's energy
	 *
	 * @param time   the timestamp of the energy change
	 * @param energy the new energy value
	 */
	public void setEnergy(long time, float energy) {
		this.currentFill.ifPresent(fill -> fill.setEnergy(time, energy));
	}

	public void saveState() {
		currentFill.ifPresent(fill -> {
			String path = getClass().getClassLoader().getResource(".").getPath();
			String full_file = path + persistenceConfiguration.applicationStateDirectory() + "/save_fill.jso";

			ObjectOutputStream oos;
			FileOutputStream fout;
			try {
				File of = new File(full_file);
				if (!of.exists()) {
					of.createNewFile();
				}
				fout = new FileOutputStream(full_file, false);
				oos = new ObjectOutputStream(fout);
				oos.writeObject(fill);
				oos.flush();
				oos.close();
			} catch (Exception ex) {
				AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_file + "   ex=" + ex);
				ex.printStackTrace();
			}

			String full_filetxt = path + persistenceConfiguration.applicationStateDirectory() + "/save_fill.txt";

			try {
				File of = new File(full_filetxt);
				if (!of.exists()) {
					of.createNewFile();
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(full_filetxt, false));
				String ans = fill.history();
				writer.write(ans);
				writer.close();
			} catch (IOException e) {

				AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_filetxt + "   ex=" + e);
			}
			AliDip2BK.log(2, "ProcData.saveState", " saved state for fill=" + fill.fillNo);
		});
	}

	public void writeFillHistFile(LhcInfoObj lhc) {
		String path = getClass().getClassLoader().getResource(".").getPath();

		String full_file = path + persistenceConfiguration.fillsHistoryDirectory() + "/fill_" + lhc.fillNo + ".txt";

		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));
			String ans = lhc.history();
			writer.write(ans);
			writer.close();
		} catch (IOException e) {

			AliDip2BK.log(4, "ProcData.writeFillHistFile", " ERROR writing file=" + full_file + "   ex=" + e);
		}
	}
}

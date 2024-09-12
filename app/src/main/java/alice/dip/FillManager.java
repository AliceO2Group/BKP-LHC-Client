package alice.dip;

import alice.dip.bookkeeping.BookkeepingClient;
import alice.dip.configuration.PersistenceConfiguration;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Optional;

public class FillManager {
	private static final String SAVE_STATE_FILE = "save_fill.jso";
	private static final String SAVE_TEXT_STATE_FILE = "save_fill.txt";

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
					" Received new FILL no=" + fillNumber + "  BUT is an active FILL =" + fill.fillNo + " Close the "
						+ "old one and created the new one"
				);
				fill.endedTime = (new Date()).getTime();
				writeFillHistFile(fill);
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

	public void loadState() {
		var savedStatePath = persistenceConfiguration.applicationStatePath().resolve(SAVE_STATE_FILE);
		if (!savedStatePath.toFile().exists()) {
			AliDip2BK.log(2, "ProcData.loadState", " No Fill State file=" + savedStatePath);
			return;
		}

		LhcInfoObj savedLhcInfo = null;

		try (var savedStateInputStream = Files.newInputStream(savedStatePath)) {
			var savedStateObjectInputStream = new ObjectInputStream(savedStateInputStream);
			savedLhcInfo = (LhcInfoObj) savedStateObjectInputStream.readObject();
		} catch (Exception e) {
			AliDip2BK.log(4, "ProcData.loadState", " ERROR loading sate from file=" + savedStatePath);
			e.printStackTrace();
		}

		if (savedLhcInfo != null) {
			AliDip2BK.log(3, "ProcData.loadState", " Loaded sate for Fill =" + savedLhcInfo.fillNo);
			currentFill = Optional.of(savedLhcInfo);
		}
	}

	public void saveState() {
		currentFill.ifPresent(fill -> {
			var savedStatePath = persistenceConfiguration.applicationStatePath().resolve(SAVE_STATE_FILE);

			try (
				var savedStateOutputStream = Files.newOutputStream(
					savedStatePath,
					StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
				)
			) {
				var savedStateObjectOutputStream = new ObjectOutputStream(savedStateOutputStream);
				savedStateObjectOutputStream.writeObject(fill);
				savedStateObjectOutputStream.flush();
			} catch (IOException e) {
				AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + savedStatePath + "   ex=" + e);
				e.printStackTrace();
			}

			var textSavedStatePath = persistenceConfiguration.applicationStatePath().resolve(SAVE_TEXT_STATE_FILE);

			try (
				var textSavedStateWriter = Files.newBufferedWriter(
					textSavedStatePath,
					StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
				)
			) {
				textSavedStateWriter.write(fill.history());
				AliDip2BK.log(2, "ProcData.saveState", " saved state for fill=" + fill.fillNo);
			} catch (IOException e) {
				AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + textSavedStatePath + "   ex=" + e);
			}
		});
	}

	public void writeFillHistFile(LhcInfoObj lhc) {
		persistenceConfiguration.fillsHistoryPath().ifPresent(path -> {
			var fillsHistoryPath = path.resolve("fill_" + lhc.fillNo + ".txt");

			try (
				var fillsHistoryWriter = Files.newBufferedWriter(
					fillsHistoryPath,
					StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
				)
			) {
				fillsHistoryWriter.write(lhc.history());
			} catch (IOException e) {
				AliDip2BK.log(
					4,
					"ProcData.writeFillHistFile",
					" ERROR writing file=" + fillsHistoryPath + "   ex=" + e
				);
			}
		});
	}
}

package alice.dip.core;

import alice.dip.bookkeeping.BookkeepingClient;
import alice.dip.configuration.PersistenceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final Logger logger = LoggerFactory.getLogger(FillManager.class);

	private Optional<LhcInfoObj> currentFill = Optional.empty();

	public FillManager(
		PersistenceConfiguration persistenceConfiguration,
		BookkeepingClient bookkeepingClient
	) {
		this.persistenceConfiguration = persistenceConfiguration;
		this.bookkeepingClient = bookkeepingClient;
	}

	public Optional<LhcInfoObj> getCurrentFill() {
		return this.currentFill;
	}

	public void setCurrentFill(LhcInfoObj currentFill) {
		this.currentFill = Optional.of(currentFill);
		logger.info("**CREATED new FILL NO={}", currentFill.fillNo);
		bookkeepingClient.createLhcFill(currentFill.getView());
		saveState();
	}

	public boolean handleFillConfigurationChanged(
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

			if (fill.fillNo == fillNumber) { // The same fill no
				if (!activeInjectionScheme.contains("no_value")) {
					boolean modification = verifyAndUpdate(fill, date, activeInjectionScheme, ip2CollisionsCount, bunchesCount);
					if (modification) {
						bookkeepingClient.updateLhcFill(fill.getView());
						saveState();
						logger.info("Update FILL NO={}", fillNumber);
					}
				} else {
					logger.error("Error updating FILL NO={} AFS={}", fillNumber, activeInjectionScheme);
				}

				return false;
			}

			logger.warn(
				"Received new FILL NO={} BUT is an active FILL={} Closed the old one and created a new one",
				fillNumber,
				fill.fillNo
			);
			fill.setEnd(new Date().getTime());
			writeFillHistFile(fill);
			bookkeepingClient.updateLhcFill(fill.getView());
		}

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

		return true;
	}

	public void setBeamMode(long date, String beamMode) {
		currentFill.ifPresentOrElse(
			fill -> {
				fill.setBeamMode(date, beamMode);
				logger.info("New beam mode={} for FILL NO={}", beamMode, fill.fillNo);
				bookkeepingClient.updateLhcFill(fill.getView());
				saveState();
			},
			() -> logger.error("ERROR new beam mode={} NO FILL NO for it", beamMode));
	}

	public void setSafeMode(long time, boolean isBeam1, boolean isBeam2) {
		currentFill.ifPresent(fill -> {
			String currentBeamMode = fill.getBeamMode();

			if (currentBeamMode.contentEquals("STABLE BEAMS")) {
				if (!isBeam1 || !isBeam2) {
					fill.setBeamMode(time, "LOST BEAMS");
					logger.warn("CHANGE BEAM MODE TO LOST BEAMS !!!");
				}
				return;
			}

			if (currentBeamMode.contentEquals("LOST BEAMS") && isBeam1 && isBeam2) {
				fill.setBeamMode(time, "STABLE BEAMS");
				logger.warn("RECOVER FROM BEAM LOST TO STABLE BEAMS");
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
			logger.info("No Fill State file={}", savedStatePath);
			return;
		}

		LhcInfoObj savedLhcInfo = null;

		try (var savedStateInputStream = Files.newInputStream(savedStatePath)) {
			var savedStateObjectInputStream = new ObjectInputStream(savedStateInputStream);
			savedLhcInfo = (LhcInfoObj) savedStateObjectInputStream.readObject();
		} catch (Exception e) {
			logger.error("ERROR loading sate from file={}", savedStatePath, e);
		}

		if (savedLhcInfo != null) {
			logger.info("Loaded sate for Fill ={}", savedLhcInfo.fillNo);
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
				logger.error("ERROR writing file={}", savedStatePath, e);
			}

			var textSavedStatePath = persistenceConfiguration.applicationStatePath().resolve(SAVE_TEXT_STATE_FILE);

			try (
				var textSavedStateWriter = Files.newBufferedWriter(
					textSavedStatePath,
					StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
				)
			) {
				textSavedStateWriter.write(fill.history());
				logger.info("Saved state for fill={}", fill.fillNo);
			} catch (IOException e) {
				logger.error("ERROR writing file={}", textSavedStatePath, e);
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
				logger.error(" ERROR writing file={}", fillsHistoryPath, e);
			}
		});
	}

	private boolean verifyAndUpdate(LhcInfoObj fill, long time, String fillingScheme, int ip2c, int nob) {
		boolean isBeamPhysicsInjection = false;
		boolean update = false;

		if (!fillingScheme.contentEquals(fill.lhcFillingSchemeName)) {
			logger.error(
				"FILL={} Filling Scheme is different OLD={} NEW={}",
				fill.fillNo,
				fill.lhcFillingSchemeName,
				fillingScheme
			);

			String beamMode = fill.getBeamMode();
			if (beamMode != null) {
				if (beamMode.contains("INJECTION") && beamMode.contains("PHYSICS")) {
					isBeamPhysicsInjection = true;
					fill.lhcFillingSchemeName = fillingScheme;
					fill.ip2CollisionsCount = ip2c;
					fill.bunchesCount = nob;
					update = true;
					logger.warn(
						"FILL={} is IPB-> Changed Filling Scheme to {}",
						fill.fillNo,
						fill.lhcFillingSchemeName
					);
				} else {
					logger.warn(
						"FILL={} is NOT in IPB keepFilling scheme to {}",
						fill.fillNo,
						fill.lhcFillingSchemeName
					);
				}
			}

			fill.saveFillingSchemeInHistory(time, fillingScheme, isBeamPhysicsInjection);
		}

		if (ip2c != fill.ip2CollisionsCount) {
			logger.warn(
				"FILL={} IP2 COLLis different OLD={} new={}",
				fill.fillNo,
				fill.ip2CollisionsCount,
				ip2c
			);
			fill.ip2CollisionsCount = ip2c;
		}

		if (nob != fill.bunchesCount) {
			logger.error(
				"FILL={} INO_BUNCHES is different OLD={} new={}",
				fill.fillNo,
				fill.bunchesCount,
				nob
			);
			fill.bunchesCount = nob;
		}

		return update;
	}
}

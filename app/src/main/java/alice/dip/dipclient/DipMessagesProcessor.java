/*************
 * cil
 **************/

package alice.dip.dipclient;

import java.math.BigInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import alice.dip.configuration.PersistenceConfiguration;
import alice.dip.core.*;
import cern.dip.BadParameter;
import cern.dip.DipData;
import cern.dip.TypeMismatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Process dip messages received from the DipClient
 * Receives DipData messages in a blocking Queue and then process them asynchronously
 * Creates Fill and Run data structures  to be stored in Alice bookkeeping system
 */
public class DipMessagesProcessor implements Runnable {
	private final BlockingQueue<MessageItem> outputQueue = new ArrayBlockingQueue<>(100);

	private final PersistenceConfiguration persistenceConfiguration;

	private final RunManager runManager;
	private final FillManager fillManager;
	private final AliceMagnetsManager aliceMagnetsManager;
	private final StatisticsManager statisticsManager;
	private final LuminosityManager luminosityManager;

	private final Logger logger = LoggerFactory.getLogger(DipMessagesProcessor.class);

	private boolean acceptData = true;

	public DipMessagesProcessor(
		PersistenceConfiguration persistenceConfiguration,
		RunManager runManager,
		FillManager fillManager,
		AliceMagnetsManager aliceMagnetsManager,
		StatisticsManager statisticsManager,
		LuminosityManager luminosityManager
	) {
		this.persistenceConfiguration = persistenceConfiguration;

		this.runManager = runManager;
		this.fillManager = fillManager;
		this.aliceMagnetsManager = aliceMagnetsManager;
		this.statisticsManager = statisticsManager;
		this.luminosityManager = luminosityManager;

		Thread t = new Thread(this);
		t.start();
	}

	/*
	 * This method is used for receiving DipData messages from the Dip Client
	 */
	public synchronized void handleMessage(String parameter, DipData data) {
		if (!acceptData) {
			logger.error(" Queue is closed ! Data from {} is NOT ACCEPTED", parameter);
			return;
		}

		MessageItem messageItem = new MessageItem(parameter, data);
		statisticsManager.incrementDipMessagesCount();

		try {
			outputQueue.put(messageItem);
		} catch (InterruptedException e) {
			logger.error("ERROR adding new data", e);
		}
	}

	// returns the length of the queue
	public int queueSize() {
		return outputQueue.size();
	}

	// used to stop the program;
	public void closeInputQueue() {
		acceptData = false;
	}

	@Override
	public void run() {
		while (true) {
			try {
				MessageItem messageItem = outputQueue.take();
				processNextInQueue(messageItem);
			} catch (InterruptedException e) {
				logger.error(" Interrupt Error", e);
			}
		}
	}

	/*
	 * This method is used to take appropriate action based on the Dip Data messages
	 */
	public void processNextInQueue(MessageItem messageItem) {
		try {
			switch ((messageItem.parameterName())) {
				case "dip/acc/LHC/RunControl/RunConfiguration":
					handleRunConfigurationMessage(messageItem.dipData());
					break;
				case "dip/acc/LHC/RunControl/SafeBeam":
					handleSafeBeamMessage(messageItem.dipData());
					break;
				case "dip/acc/LHC/Beam/Energy":
					handleEnergyMessage(messageItem.dipData());
					break;
				case "dip/acc/LHC/RunControl/BeamMode":
					handleBeamModeMessage(messageItem.dipData());
					break;
				case "dip/acc/LHC/Beam/BetaStar/Bstar2":
					handleBetaStarMessage(messageItem.dipData());
					break;
				case "dip/ALICE/MCS/Solenoid/Current":
					handleL3CurrentMessage(messageItem.dipData());
					break;
				case "dip/ALICE/MCS/Dipole/Current":
					handleDipoleCurrentMessage(messageItem.dipData());
					break;
				case "dip/ALICE/MCS/Solenoid/Polarity":
					handleL3PolarityMessage(messageItem.dipData());
					break;
				case "dip/ALICE/MCS/Dipole/Polarity":
					handleDipolePolarityMessage(messageItem.dipData());
					break;
				case "dip/ALICE/LHC/Bookkeeping/Source":
					handleBookkeepingSourceMessage(messageItem.dipData());
					break;
				case "dip/ALICE/LHC/Bookkeeping/CTPClock":
					handleBookkeepingCtpClockMessage(messageItem.dipData());
					break;
				default:
					logger.error("!!!!!!!!!! Unimplemented Data Process for P={}", messageItem.parameterName());
			}
		} catch (Exception e) {
			logger.error("ERROR processing DIP message P={}", messageItem.parameterName(), e);
		}
	}

	private void handleRunConfigurationMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var fillNumberStr = dipData.extractString("FILL_NO");
		var beam1ParticleType = dipData.extractString("PARTICLE_TYPE_B1");
		var beam2ParticleType = dipData.extractString("PARTICLE_TYPE_B2");
		var activeInjectionScheme = dipData.extractString("ACTIVE_INJECTION_SCHEME");
		var ip2CollisionsCountStr = dipData.extractString("IP2-NO-COLLISIONS");
		var bunchesCountStr = dipData.extractString("NO_BUNCHES");

		var time = dipData.extractDipTime().getAsMillis();

		logger.debug(
			"Handle run configuration FILL={} AIS={} IP2_COLL={}",
			fillNumberStr,
			activeInjectionScheme,
			ip2CollisionsCountStr
		);

		int fillNumber;
		try {
			fillNumber = Integer.parseInt(fillNumberStr);
		} catch (NumberFormatException e) {
			logger.error("ERROR parse INT for fillNo={}", fillNumberStr, e);
			return;
		}

		int ip2CollisionsCount;
		try {
			ip2CollisionsCount = Integer.parseInt(ip2CollisionsCountStr);
		} catch (NumberFormatException e) {
			logger.error("ERROR parse INT for IP2_COLLISIONS={}", ip2CollisionsCountStr, e);
			return;
		}

		int bunchesCount;
		try {
			bunchesCount = Integer.parseInt(bunchesCountStr);
		} catch (NumberFormatException e) {
			logger.error("ERROR parse INT for NO_BUNCHES={}", bunchesCountStr, e);
			return;
		}

		var isNewFill = fillManager.handleFillConfigurationChanged(
			time,
			fillNumber,
			beam1ParticleType,
			beam2ParticleType,
			activeInjectionScheme,
			ip2CollisionsCount,
			bunchesCount
		);

		if (isNewFill) statisticsManager.incrementNewFillsCount();
	}

	private void handleSafeBeamMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var safeModePayload = dipData.extractInt("payload");

		var time = dipData.extractDipTime().getAsMillis();

		var isBeam1 = BigInteger.valueOf(safeModePayload).testBit(0);
		var isBeam2 = BigInteger.valueOf(safeModePayload).testBit(4);
		var isStableBeams = BigInteger.valueOf(safeModePayload).testBit(2);

		logger.debug(
			"New safeBeam message VAL={} isB1={} isB2={} isSB={}",
			safeModePayload,
			isBeam1,
			isBeam2,
			isStableBeams
		);

		fillManager.setSafeMode(time, isBeam1, isBeam2);
	}

	private void handleEnergyMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var energyPayload = dipData.extractInt("payload");
		// Per documentation, value has to be multiplied by 120 go get MeV
		// https://confluence.cern.ch/display/expcomm/Energy
		var energy = 0.12f * energyPayload; // We use energy in keV

		var time = dipData.extractDipTime().getAsMillis();

		fillManager.setEnergy(time, energy);

		if (persistenceConfiguration.saveParametersHistoryPerRun()) {
			runManager.registerNewEnergy(time, energy);
		}
	}

	private void handleBeamModeMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var beamMode = dipData.extractString("value");

		var time = dipData.extractDipTime().getAsMillis();

		logger.debug("New Beam Mode={}", beamMode);
		fillManager.setBeamMode(time, beamMode);
	}

	private void handleBetaStarMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var betaStarPayload = dipData.extractInt("payload");
		// Per documentation, value is in cm, we convert it to m
		var betaStar = betaStarPayload / 1000.f; // in m

		var time = dipData.extractDipTime().getAsMillis();

		fillManager.setBetaStar(time, betaStar);
	}

	private void handleL3CurrentMessage(DipData dipData) throws TypeMismatch {
		var current = dipData.extractFloat();

		var time = dipData.extractDipTime().getAsMillis();

		aliceMagnetsManager.setL3Current(current);

		if (persistenceConfiguration.saveParametersHistoryPerRun()) {
			runManager.registerNewL3MagnetCurrent(time, current);
		}
	}

	private void handleDipoleCurrentMessage(DipData dipData) throws TypeMismatch {
		var current = dipData.extractFloat();

		var time = dipData.extractDipTime().getAsMillis();

		aliceMagnetsManager.setDipoleCurrent(current);

		if (persistenceConfiguration.saveParametersHistoryPerRun()) {
			runManager.registerNewDipoleCurrent(time, current);
		}
	}

	private void handleL3PolarityMessage(DipData dipData) throws TypeMismatch {
		var isNegative = dipData.extractBoolean();
		var polarity = isNegative ? Polarity.NEGATIVE : Polarity.POSITIVE;

		aliceMagnetsManager.setL3Polarity(polarity);

		logger.info("L3 Polarity={}", polarity);
	}

	private void handleDipolePolarityMessage(DipData dipData) throws TypeMismatch {
		var isNegative = dipData.extractBoolean();
		var polarity = isNegative ? Polarity.NEGATIVE : Polarity.POSITIVE;

		aliceMagnetsManager.setDipolePolarity(polarity);

		logger.info("Dipole Polarity={}", polarity);
	}

	private void handleBookkeepingSourceMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var acceptance = dipData.extractFloat("Acceptance");
		var crossSection = dipData.extractFloat("CrossSection");
		var efficiency = dipData.extractFloat("Efficiency");

		luminosityManager.setTriggerEfficiency(efficiency);
		luminosityManager.setTriggerAcceptance(acceptance);
		luminosityManager.setCrossSection(crossSection);
	}

	private void handleBookkeepingCtpClockMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var phaseShiftBeam1 = dipData.extractFloat("PhaseShift_Beam1");
		var phaseShiftBeam2 = dipData.extractFloat("PhaseShift_Beam2");

		luminosityManager.setPhaseShift(new PhaseShift(phaseShiftBeam1, phaseShiftBeam2));
	}
}

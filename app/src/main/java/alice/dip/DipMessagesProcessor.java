/*************
 * cil
 **************/

package alice.dip;

import java.math.BigInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import alice.dip.bookkeeping.BookkeepingClient;
import alice.dip.bookkeeping.BookkeepingRunUpdatePayload;
import alice.dip.configuration.PersistenceConfiguration;
import cern.dip.BadParameter;
import cern.dip.DipData;
import cern.dip.TypeMismatch;

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

	private boolean acceptData = true;

	public DipMessagesProcessor(
		PersistenceConfiguration persistenceConfiguration,
		RunManager runManager,
		FillManager fillManager,
		AliceMagnetsManager aliceMagnetsManager,
		StatisticsManager statisticsManager
	) {
		this.persistenceConfiguration = persistenceConfiguration;

		this.runManager = runManager;
		this.fillManager = fillManager;
		this.aliceMagnetsManager = aliceMagnetsManager;
		this.statisticsManager = statisticsManager;

		Thread t = new Thread(this);
		t.start();
	}

	/*
	 * This method is used for receiving DipData messages from the Dip Client
	 */
	public synchronized void handleMessage(String parameter, String message, DipData data) {
		if (!acceptData) {
			AliDip2BK.log(4, "ProcData.addData", " Queue is closed ! Data from " + parameter + " is NOT ACCEPTED");
			return;
		}

		MessageItem messageItem = new MessageItem(parameter, message, data);
		statisticsManager.incrementDipMessagesCount();

		try {
			outputQueue.put(messageItem);
		} catch (InterruptedException e) {
			AliDip2BK.log(4, "ProcData.addData", "ERROR adding new data ex= " + e);
			e.printStackTrace();
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
				AliDip2BK.log(4, "ProcData.run", " Interrupt Error=" + e);
				e.printStackTrace();
			}
		}
	}

	/*
	 * This method is used to take appropriate action based on the Dip Data messages
	 */
	public void processNextInQueue(MessageItem messageItem) {
		try {
			switch ((messageItem.param_name)) {
				case "dip/acc/LHC/RunControl/RunConfiguration":
					handleRunConfigurationMessage(messageItem.data);
					break;
				case "dip/acc/LHC/RunControl/SafeBeam":
					handleSafeBeamMessage(messageItem.data);
					break;
				case "dip/acc/LHC/Beam/Energy":
					handleEnergyMessage(messageItem.data);
					break;
				case "dip/acc/LHC/RunControl/BeamMode":
					handleBeamModeMessage(messageItem.data);
					break;
				case "dip/acc/LHC/Beam/BetaStar/Bstar2":
					handleBetaStarMessage(messageItem.data);
					break;
				case "dip/ALICE/MCS/Solenoid/Current":
					handleL3CurrentMessage(messageItem.data);
					break;
				case "dip/ALICE/MCS/Dipole/Current":
					handleDipoleCurrentMessage(messageItem.data);
					break;
				case "dip/ALICE/MCS/Solenoid/Polarity":
					handleL3PolarityMessage(messageItem.data);
					break;
				case "dip/ALICE/MCS/Dipole/Polarity":
					handleDipolePolarityMessage(messageItem.data);
					break;
				default:
					AliDip2BK.log(
						4,
						"ProcData.dispach",
						"!!!!!!!!!! Unimplemented Data Process for P=" + messageItem.param_name
					);
			}
		} catch (Exception e) {
			AliDip2BK.log(
				2,
				"ProcData.dispach",
				" ERROR processing DIP message P=" + messageItem.param_name + " ex=" + e
			);
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

		AliDip2BK.log(
			1,
			"ProcData.dispach",
			" RunConfigurttion  FILL No = " + fillNumberStr + "  AIS=" + activeInjectionScheme + " IP2_COLL="
				+ ip2CollisionsCountStr
		);

		int fillNumber;
		try {
			fillNumber = Integer.parseInt(fillNumberStr);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(4, "ProcData.newFILL", "ERROR parse INT for fillNo= " + fillNumberStr);
			return;
		}

		int ip2CollisionsCount;
		try {
			ip2CollisionsCount = Integer.parseInt(ip2CollisionsCountStr);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for IP2_COLLISIONS= " + ip2CollisionsCountStr);
			return;
		}

		int bunchesCount;
		try {
			bunchesCount = Integer.parseInt(bunchesCountStr);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for NO_BUNCHES= " + bunchesCountStr);
			return;
		}

		fillManager.handleFillConfigurationChanged(
			time,
			fillNumber,
			beam1ParticleType,
			beam2ParticleType,
			activeInjectionScheme,
			ip2CollisionsCount,
			bunchesCount
		);
	}

	private void handleSafeBeamMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var safeModePayload = dipData.extractInt("payload");

		var time = dipData.extractDipTime().getAsMillis();

		var isBeam1 = BigInteger.valueOf(safeModePayload).testBit(0);
		var isBeam2 = BigInteger.valueOf(safeModePayload).testBit(4);
		var isStableBeams = BigInteger.valueOf(safeModePayload).testBit(2);

		AliDip2BK.log(
			0,
			"ProcData.newSafeBeams",
			" VAL=" + safeModePayload + " isB1=" + isBeam1 + " isB2=" + isBeam2 + " isSB=" + isStableBeams
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

		AliDip2BK.log(1, "ProcData.dispach", " New Beam MOde = " + beamMode);
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

		AliDip2BK.log(2, "ProcData.dispach", " L3 Polarity=" + polarity);
	}

	private void handleDipolePolarityMessage(DipData dipData) throws TypeMismatch {
		var isNegative = dipData.extractBoolean();
		var polarity = isNegative ? Polarity.NEGATIVE : Polarity.POSITIVE;

		aliceMagnetsManager.setDipolePolarity(polarity);

		AliDip2BK.log(2, "ProcData.dispach", " Dipole Polarity=" + polarity);
	}
}

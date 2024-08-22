/*************
 * cil
 **************/

package alice.dip;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
	private final BookkeepingClient bookkeepingClient;
	private final RunManager runManager;
	private final FillManager fillManager;
	private final AliceMagnetsManager aliceMagnetsManager;
	private final StatisticsManager statisticsManager;

	private boolean acceptData = true;

	public DipMessagesProcessor(
		BookkeepingClient bookkeepingClient,
		RunManager runManager,
		FillManager fillManager,
		AliceMagnetsManager aliceMagnetsManager,
		StatisticsManager statisticsManager
	) {
		this.bookkeepingClient = bookkeepingClient;
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
	synchronized public void handleMessage(String parameter, String message, DipData data) {
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

		if (AliDip2BK.OUTPUT_FILE != null) {
			String file = AliDip2BK.ProgPath + AliDip2BK.OUTPUT_FILE;
			try {
				File of = new File(file);
				if (!of.exists()) {
					of.createNewFile();
				}
				BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

				writer.write("=>" + messageItem.format_message + "\n");
				writer.close();
			} catch (IOException e) {
				AliDip2BK.log(1, "ProcData.addData", "ERROR write data to dip output log  data ex= " + e);
			}
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

	public synchronized void newRunSignal(long date, int runNumber) {
		statisticsManager.incrementNewRunsCount();
		statisticsManager.incrementKafkaMessagesCount();

		if (!runManager.hasRunByRunNumber(runNumber)) {
			var currentFill = fillManager.getCurrentFill();
			var newRun = new RunInfoObj(
				date,
				runNumber,
				currentFill.map(LhcInfoObj::clone).orElse(null),
				aliceMagnetsManager.getView()
			);
			var fillLogMessage = currentFill.map(lhcInfoObj -> "with FillNo=" + lhcInfoObj.fillNo)
				.orElse("currentFILL is NULL Perhaps Cosmics Run");
			AliDip2BK.log(2, "ProcData.newRunSignal", " NEW RUN NO =" + runNumber + fillLogMessage);

			runManager.addRun(newRun);
			bookkeepingClient.updateRun(newRun);

			if (currentFill.isPresent()) {
				var lastRunNumber = runManager.getLastRunNumber();

				// Check if there is the new run is right after the last one
				if (lastRunNumber.isPresent() && runNumber - lastRunNumber.getAsInt() != 1) {
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

				runManager.setLastRunNumber(runNumber);
			}
		} else {
			AliDip2BK.log(6, "ProcData.newRunSignal", " Duplicate new  RUN signal =" + runNumber + " IGNORE it");
		}
	}

	public synchronized void stopRunSignal(long time, int runNumber) {
		statisticsManager.incrementKafkaMessagesCount();

		var currentRun = runManager.getRunByRunNumber(runNumber);

		currentRun.ifPresentOrElse(run -> {
			run.setEORtime(time);
			fillManager.getCurrentFill().ifPresent(fill -> run.LHC_info_stop = fill.clone());
			run.alice_info_stop = aliceMagnetsManager.getView();

			runManager.endRun(run.RunNo);
		}, () -> {
			statisticsManager.incrementDuplicatedRunsEndCount();
			AliDip2BK.log(4, "ProcData.stopRunSignal", " NO ACTIVE RUN having runNo=" + runNumber);
		});
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

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
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

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			runManager.registerNewL3MagnetCurrent(time, current);
		}
	}

	private void handleDipoleCurrentMessage(DipData dipData) throws TypeMismatch {
		var current = dipData.extractFloat();

		var time = dipData.extractDipTime().getAsMillis();

		aliceMagnetsManager.setDipoleCurrent(current);

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
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

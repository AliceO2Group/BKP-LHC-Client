/*************
 * cil
 **************/

package alice.dip;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.Date;
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
	private final StatisticsManager statisticsManager;

	private boolean acceptData = true;

	private LhcInfoObj currentFill = null;
	private final AliceInfoObj currentAlice;

	public DipMessagesProcessor(
		BookkeepingClient bookkeepingClient, RunManager runManager, StatisticsManager statisticsManager
	) {
		this.bookkeepingClient = bookkeepingClient;
		this.runManager = runManager;
		this.statisticsManager = statisticsManager;

		Thread t = new Thread(this);
		t.start();

		currentAlice = new AliceInfoObj();
		loadState();
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
			if (currentFill != null) {
				RunInfoObj newRun = new RunInfoObj(date, runNumber, currentFill.clone(), currentAlice.clone());
				runManager.addRun(newRun);
				AliDip2BK.log(
					2,
					"ProcData.newRunSignal",
					" NEW RUN NO =" + runNumber + "  with FillNo=" + currentFill.fillNo
				);
				bookkeepingClient.updateRun(newRun);

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
						" LOST RUN No Signal! " + missingRunsList + "  New RUN NO =" + runNumber + " Last Run No="
							+ lastRunNumber
					);
				}

				runManager.setLastRunNumber(runNumber);
			} else {
				RunInfoObj newRun = new RunInfoObj(date, runNumber, null, currentAlice.clone());
				runManager.addRun(newRun);
				AliDip2BK.log(
					2,
					"ProcData.newRunSignal",
					" NEW RUN NO =" + runNumber + " currentFILL is NULL Perhaps Cosmics Run"
				);
				bookkeepingClient.updateRun(newRun);
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
			if (currentFill != null) run.LHC_info_stop = currentFill.clone();
			run.alice_info_stop = currentAlice.clone();

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

		newFillNo(
			time,
			fillNumberStr,
			beam1ParticleType,
			beam2ParticleType,
			activeInjectionScheme,
			ip2CollisionsCountStr,
			bunchesCountStr
		);
	}

	private void handleSafeBeamMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var safeModePayload = dipData.extractInt("payload");

		var time = dipData.extractDipTime().getAsMillis();

		var isBeam1 = BigInteger.valueOf(safeModePayload).testBit(0);
		var isBeam2 = BigInteger.valueOf(safeModePayload).testBit(4);
		var isStableBeams = BigInteger.valueOf(safeModePayload).testBit(2);

		if (currentFill == null) return;

		AliDip2BK.log(
			0,
			"ProcData.newSafeBeams",
			" VAL=" + safeModePayload + " isB1=" + isBeam1 + " isB2=" + isBeam2 + " isSB=" + isStableBeams
		);

		String bm = currentFill.getBeamMode();

		if (bm.contentEquals("STABLE BEAMS")) {

			if (!isBeam1 || !isBeam2) {
				currentFill.setBeamMode(time, "LOST BEAMS");
				AliDip2BK.log(5, "ProcData.newSafeBeams", " CHANGE BEAM MODE TO LOST BEAMS !!! ");
			}

			return;
		}

		if (bm.contentEquals("LOST BEAMS") && isBeam1 && isBeam2) {
			currentFill.setBeamMode(time, "STABLE BEAMS");
			AliDip2BK.log(5, "ProcData.newSafeBeams", " RECOVER FROM BEAM LOST TO STABLE BEAMS ");
		}
	}

	private void handleEnergyMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var energyPayload = dipData.extractInt("payload");
		// Per documentation, value has to be multiplied by 120 go get MeV
		// https://confluence.cern.ch/display/expcomm/Energy
		var energy = (float) (0.12 * (float) energyPayload); // We use energy in keV

		var time = dipData.extractDipTime().getAsMillis();

		if (currentFill != null) {
			currentFill.setEnergy(time, energy);
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			runManager.registerNewEnergy(time, energy);
		}
	}

	private void handleBeamModeMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var beamMode = dipData.extractString("value");

		var time = dipData.extractDipTime().getAsMillis();

		AliDip2BK.log(1, "ProcData.dispach", " New Beam MOde = " + beamMode);
		newBeamMode(time, beamMode);
	}

	private void handleBetaStarMessage(DipData dipData) throws BadParameter, TypeMismatch {
		var betaStarPayload = dipData.extractInt("payload");
		// Per documentation, value is in cm, we convert it to m
		var betaStar = betaStarPayload / 1000.f; // in m

		var time = dipData.extractDipTime().getAsMillis();

		if (currentFill != null) {
			currentFill.setLHCBetaStar(time, betaStar);
		}
	}

	private void handleL3CurrentMessage(DipData dipData) throws TypeMismatch {
		var current = dipData.extractFloat();

		var time = dipData.extractDipTime().getAsMillis();

		if (currentAlice != null) {
			currentAlice.L3_magnetCurrent = current;
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			runManager.registerNewL3MagnetCurrent(time, current);
		}
	}

	private void handleDipoleCurrentMessage(DipData dipData) throws TypeMismatch {
		var current = dipData.extractFloat();

		var time = dipData.extractDipTime().getAsMillis();

		if (currentAlice != null) {
			currentAlice.Dipole_magnetCurrent = current;
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			runManager.registerNewDipoleCurrent(time, current);
		}
	}

	private void handleL3PolarityMessage(DipData dipData) throws TypeMismatch {
		var isNegative = dipData.extractBoolean();

		if (isNegative) {
			currentAlice.L3_polarity = "Negative";
		} else {
			currentAlice.L3_polarity = "Positive";
		}

		AliDip2BK.log(2, "ProcData.dispach", " L3 Polarity=" + currentAlice.L3_polarity);
	}

	private void handleDipolePolarityMessage(DipData dipData) throws TypeMismatch {
		var isNegative = dipData.extractBoolean();

		if (isNegative) {
			currentAlice.Dipole_polarity = "Negative";
		} else {
			currentAlice.Dipole_polarity = "Positive";
		}

		AliDip2BK.log(2, "ProcData.dispach", " Dipole Polarity=" + currentAlice.Dipole_polarity);
	}

	public void newFillNo(long date, String strFno, String par1, String par2, String ais, String strIP2, String strNB) {
		int no = -1;
		int ip2Col = 0;
		int nob = 0;

		try {
			no = Integer.parseInt(strFno);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(4, "ProcData.newFILL", "ERROR parse INT for fillNo= " + strFno);
			return;
		}

		try {
			ip2Col = Integer.parseInt(strIP2);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for IP2_COLLISIONS= " + strIP2);
		}

		try {
			nob = Integer.parseInt(strNB);
		} catch (NumberFormatException e1) {
			AliDip2BK.log(3, "ProcData.newFILL", "ERROR parse INT for NO_BUNCHES= " + strIP2);
		}

		if (currentFill == null) {
			currentFill = new LhcInfoObj(date, no, par1, par2, ais, ip2Col, nob);
			bookkeepingClient.createLhcFill(currentFill);
			saveState();
			AliDip2BK.log(2, "ProcData.newFillNo", " **CREATED new FILL no=" + no);
			statisticsManager.incrementNewFillsCount();
			return;
		}
		if (currentFill.fillNo == no) { // the same fill no ;
			if (!ais.contains("no_value")) {
				boolean modi = currentFill.verifyAndUpdate(date, no, ais, ip2Col, nob);
				if (modi) {
					bookkeepingClient.updateLhcFill(currentFill);
					saveState();
					AliDip2BK.log(2, "ProcData.newFillNo", " * Update FILL no=" + no);
				}
			} else {
				AliDip2BK.log(4, "ProcData.newFillNo", " * FILL no=" + no + " AFS=" + ais);
			}
		} else {
			AliDip2BK.log(
				3,
				"ProcData.newFillNo",
				" Received new FILL no=" + no + "  BUT is an active FILL =" + currentFill.fillNo + " Close the old "
					+ "one" + " and created the new one"
			);
			currentFill.endedTime = (new Date()).getTime();
			if (AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY != null) {
				writeFillHistFile(currentFill);
			}
			bookkeepingClient.updateLhcFill(currentFill);

			currentFill = null;
			currentFill = new LhcInfoObj(date, no, par1, par2, ais, ip2Col, nob);
			bookkeepingClient.createLhcFill(currentFill);
			statisticsManager.incrementNewFillsCount();
			saveState();
		}
	}

	public void newBeamMode(long date, String beamMode) {
		if (currentFill != null) {
			currentFill.setBeamMode(date, beamMode);

			int mc = -1;
			for (int i = 0; i < AliDip2BK.endFillCases.length; i++) {
				if (AliDip2BK.endFillCases[i].equalsIgnoreCase(beamMode)) mc = i;
			}
			if (mc < 0) {

				AliDip2BK.log(
					2,
					"ProcData.newBeamMode",
					"New beam mode=" + beamMode + "  for FILL_NO=" + currentFill.fillNo
				);
				bookkeepingClient.updateLhcFill(currentFill);
				saveState();
			} else {
				currentFill.endedTime = date;
				bookkeepingClient.updateLhcFill(currentFill);
				if (AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY != null) {
					writeFillHistFile(currentFill);
				}
				AliDip2BK.log(
					3,
					"ProcData.newBeamMode",
					"CLOSE Fill_NO=" + currentFill.fillNo + " Based on new  beam mode=" + beamMode
				);
				currentFill = null;
			}
		} else {
			AliDip2BK.log(4, "ProcData.newBeamMode", " ERROR new beam mode=" + beamMode + " NO FILL NO for it");
		}
	}

	public void saveState() {
		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + AliDip2BK.KEEP_STATE_DIR + "/save_fill.jso";

		ObjectOutputStream oos = null;
		FileOutputStream fout = null;
		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			fout = new FileOutputStream(full_file, false);
			oos = new ObjectOutputStream(fout);
			oos.writeObject(currentFill);
			oos.flush();
			oos.close();
		} catch (Exception ex) {
			AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_file + "   ex=" + ex);
			ex.printStackTrace();
		}

		String full_filetxt = path + AliDip2BK.KEEP_STATE_DIR + "/save_fill.txt";

		try {
			File of = new File(full_filetxt);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_filetxt, false));
			String ans = currentFill.history();
			writer.write(ans);
			writer.close();
		} catch (IOException e) {

			AliDip2BK.log(4, "ProcData.saveState", " ERROR writing file=" + full_filetxt + "   ex=" + e);
		}
		AliDip2BK.log(2, "ProcData.saveState", " saved state for fill=" + currentFill.fillNo);
	}

	public void loadState() {
		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + AliDip2BK.KEEP_STATE_DIR + "/save_fill.jso";

		File of = new File(full_file);
		if (!of.exists()) {
			AliDip2BK.log(2, "ProcData.loadState", " No Fill State file=" + full_file);
			return;
		}

		ObjectInputStream objectinputstream = null;
		try {
			FileInputStream streamIn = new FileInputStream(full_file);
			streamIn = new FileInputStream(full_file);
			objectinputstream = new ObjectInputStream(streamIn);
			LhcInfoObj slhc = null;
			slhc = (LhcInfoObj) objectinputstream.readObject();
			objectinputstream.close();
			if (slhc != null) {
				AliDip2BK.log(3, "ProcData.loadState", " Loaded sate for Fill =" + slhc.fillNo);
				currentFill = slhc;
			}
		} catch (Exception e) {
			AliDip2BK.log(4, "ProcData.loadState", " ERROR Loaded sate from file=" + full_file);
			e.printStackTrace();
		}
	}

	public void writeFillHistFile(LhcInfoObj lhc) {
		String path = getClass().getClassLoader().getResource(".").getPath();

		String full_file = path + AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY + "/fill_" + lhc.fillNo + ".txt";

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

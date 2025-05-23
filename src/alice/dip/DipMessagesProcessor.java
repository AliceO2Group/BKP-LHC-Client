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
import java.util.ArrayList;
import java.util.Date;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import cern.dip.BadParameter;
import cern.dip.DipData;
import cern.dip.DipTimestamp;
import cern.dip.TypeMismatch;

/*
 * Process dip messages received from the DipClient
 * Receives DipData messages in a blocking Queue and then process them asynchronously
 * Creates Fill and Run data structures  to be stored in Alice bookkeeping system
 */
public class DipMessagesProcessor implements Runnable {
	public BookkeepingClient bookkeepingClient;
	public int statNoDipMess = 0;
	public int statNoKafMess = 0;
	public int statNoNewFills = 0;
	public int statNoNewRuns = 0;
	public int statNoEndRuns = 0;
	public int statNoDuplicateEndRuns = 0;
	public int LastRunNumber = -1;
	boolean acceptData = true;
	LhcInfoObj currentFill = null;
	AliceInfoObj currentAlice = null;
	SimDipEventsFill simFill;
	ArrayList<RunInfoObj> ActiveRuns = new ArrayList<RunInfoObj>();
	private BlockingQueue<MessageItem> outputQueue = new ArrayBlockingQueue<MessageItem>(100);

	private final LuminosityManager luminosityManager;

	public DipMessagesProcessor(BookkeepingClient bookkeepingClient, LuminosityManager luminosityManager) {

		this.bookkeepingClient = bookkeepingClient;
		this.luminosityManager = luminosityManager;

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
		statNoDipMess = statNoDipMess + 1;

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

		RunInfoObj rio = getRunNo(runNumber);
		statNoNewRuns = statNoNewRuns + 1;
		statNoKafMess = statNoKafMess + 1;

		if (rio == null) {
			if (currentFill != null) {
				RunInfoObj newrun = new RunInfoObj(
						date,
						runNumber,
						currentFill.clone(),
						currentAlice.clone(),
						luminosityManager.getView()
				);
				ActiveRuns.add(newrun);
				AliDip2BK.log(
						2,
						"ProcData.newRunSignal",
						" NEW RUN NO =" + runNumber + "  with FillNo=" + currentFill.fillNo
				);
				bookkeepingClient.updateRun(newrun);

				if (LastRunNumber == -1) {
					LastRunNumber = runNumber;
				} else {
					int drun = runNumber - LastRunNumber;
					if (drun == 1) {
						LastRunNumber = runNumber;
					} else {
						String llist = "<<";
						for (int ij = (LastRunNumber + 1); ij < runNumber; ij++) {
							llist = llist + ij + " ";
						}
						llist = llist + ">>";

						AliDip2BK.log(
								7,
								"ProcData.newRunSignal",
								" LOST RUN No Signal! " + llist + "  New RUN NO =" + runNumber + " Last Run No=" + LastRunNumber
						);
						LastRunNumber = runNumber;
					}
				}
			} else {
				RunInfoObj newrun = new RunInfoObj(
						date,
						runNumber,
						null,
						currentAlice.clone(),
						luminosityManager.getView()
				);
				ActiveRuns.add(newrun);
				AliDip2BK.log(
						2,
						"ProcData.newRunSignal",
						" NEW RUN NO =" + runNumber + " currentFILL is NULL Perhaps Cosmics Run"
				);
				bookkeepingClient.updateRun(newrun);
			}
		} else {
			AliDip2BK.log(6, "ProcData.newRunSignal", " Duplicate new  RUN signal =" + runNumber + " IGNORE it");
		}
	}

	public synchronized void stopRunSignal(long time, int runNumber) {

		statNoKafMess = statNoKafMess + 1;

		RunInfoObj rio = getRunNo(runNumber);

		if (rio != null) {

			rio.setEORtime(time);
			if (currentFill != null) rio.LHC_info_stop = currentFill.clone();
			rio.alice_info_stop = currentAlice.clone();
			rio.setLuminosityAtStop(luminosityManager.getView());
			bookkeepingClient.updateRun(rio);

			EndRun(rio);
		} else {
			statNoDuplicateEndRuns = statNoDuplicateEndRuns + 1;
			AliDip2BK.log(4, "ProcData.stopRunSignal", " NO ACTIVE RUN having runNo=" + runNumber);
		}
	}

	/*
	 * This method is used to take appropriate action based on the Dip Data messages
	 */
	public void processNextInQueue(MessageItem messageItem) {
		if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/RunConfiguration")) {
			String ans = Util.parseDipMess(messageItem.param_name, messageItem.data);
			try {
				String fillno = messageItem.data.extractString("FILL_NO");
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();

				String par1 = messageItem.data.extractString("PARTICLE_TYPE_B1");
				String par2 = messageItem.data.extractString("PARTICLE_TYPE_B2");
				String ais = messageItem.data.extractString("ACTIVE_INJECTION_SCHEME");
				String strIP2_NO_COLLISIONS = messageItem.data.extractString("IP2-NO-COLLISIONS");
				String strNO_BUNCHES = messageItem.data.extractString("NO_BUNCHES");

				AliDip2BK.log(
						1,
						"ProcData.dispach",
						" RunConfigurttion  FILL No = " + fillno + "  AIS=" + ais + " IP2_COLL=" + strIP2_NO_COLLISIONS
				);

				newFillNo(time, fillno, par1, par2, ais, strIP2_NO_COLLISIONS, strNO_BUNCHES);
			} catch (Exception e) {
				AliDip2BK.log(
						4,
						"ProcData.dispach",
						" ERROR in RunConfiguration P=" + messageItem.param_name + "  Ans=" + ans + " ex=" + e
				);
			}
			// SafeBeam
		} else if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/SafeBeam")) {

			try {
				int v = messageItem.data.extractInt("payload");
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();

				newSafeMode(time, v);
			} catch (Exception e) {
				// AliDip2BK.log(1, "ProcData.dispach" ," ERROR on SafeBeam P="+ mes.param_name + " ex=" +e );

			}

			// Energy
		} else if (messageItem.param_name.contentEquals("dip/acc/LHC/Beam/Energy")) {
			try {
				int v = messageItem.data.extractInt("payload");
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();

				newEnergy(time, (float) (0.12 * (float) v));
			} catch (Exception e) {
				AliDip2BK.log(1, "ProcData.dispach", " ERROR on Energy P=" + messageItem.param_name + " ex=" + e);
			}
			// Beam Mode
		} else if (messageItem.param_name.contentEquals("dip/acc/LHC/RunControl/BeamMode")) {

			try {
				String v = messageItem.data.extractString("value");
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();

				AliDip2BK.log(1, "ProcData.dispach", " New Beam MOde = " + v);
				newBeamMode(time, v);
			} catch (Exception e) {
				AliDip2BK.log(3, "ProcData.dispach", " ERROR on Beam MOde on P=" + messageItem.param_name + " ex=" + e);
				// e.printStackTrace();
			}
		} else if (messageItem.param_name.contentEquals("dip/acc/LHC/Beam/BetaStar/Bstar2")) {
			try {
				int v = messageItem.data.extractInt("payload");
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();

				double v1 = (double) v;

				double v2 = v1 / 1000.0; // in m

				newBetaStar(time, (float) v2);
			} catch (Exception e) {
				AliDip2BK.log(1, "ProcData.dispach", " ERROR on BetaStar  P=" + messageItem.param_name + " ex=" + e);
				// e.printStackTrace();
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Solenoid/Current")) {

			try {
				float v = messageItem.data.extractFloat();
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();
				newL3magnetCurrent(time, v);
			} catch (Exception e) {
				AliDip2BK.log(
						2,
						"ProcData.dispach",
						" ERROR on Solenoid Curr P=" + messageItem.param_name + " ex=" + e
				);
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Dipole/Current")) {

			try {
				float v = messageItem.data.extractFloat();
				DipTimestamp dptime = messageItem.data.extractDipTime();
				long time = dptime.getAsMillis();
				newDipoleCurrent(time, v);
			} catch (Exception e) {
				AliDip2BK.log(
						2,
						"ProcData.dispach",
						" ERROR on Dipole Curr on P=" + messageItem.param_name + " ex=" + e
				);
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Solenoid/Polarity")) {

			try {
				boolean v = messageItem.data.extractBoolean();
				// DipTimestamp dptime =mes.data.extractDipTime();
				// long time =dptime.getAsMillis();

				if (v) {
					currentAlice.L3_polarity = "Negative";
				} else {
					currentAlice.L3_polarity = "Positive";
				}

				AliDip2BK.log(2, "ProcData.dispach", " L3 Polarity=" + currentAlice.L3_polarity);
			} catch (Exception e) {
				AliDip2BK.log(2, "ProcData.dispach", " ERROR on L3 polarity P=" + messageItem.param_name + " ex=" + e);
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/MCS/Dipole/Polarity")) {

			try {
				boolean v = messageItem.data.extractBoolean();
				// DipTimestamp dptime =mes.data.extractDipTime();
				// long time =dptime.getAsMillis();
				if (v) currentAlice.Dipole_polarity = "Negative";

				else {
					currentAlice.Dipole_polarity = "Positive";
				}

				AliDip2BK.log(2, "ProcData.dispach", " Dipole Polarity=" + currentAlice.Dipole_polarity);
			} catch (Exception e) {
				AliDip2BK.log(
						2,
						"ProcData.dispach",
						" ERROR on Dipole Polarity P=" + messageItem.param_name + " ex=" + e
				);
				// e.printStackTrace();
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/LHC/Bookkeeping/Source")) {
			try {
				handleBookkeepingSourceMessage(messageItem.data);
			} catch (Exception e) {
				AliDip2BK.log(2, "ProcData.dispach", " ERROR on BKP source=" + messageItem.param_name + " ex=" + e);
			}
		} else if (messageItem.param_name.contentEquals("dip/ALICE/LHC/Bookkeeping/CTPClock")) {
			try {
				handleBookkeepingCtpClockMessage(messageItem.data);
				AliDip2BK.log(2, "ProcData.dispach", " L3 Polarity=" + currentAlice.L3_polarity);
			} catch (Exception e) {
				AliDip2BK.log(2, "ProcData.dispach", " ERROR on BKP CTPClock=" + messageItem.param_name + " ex=" + e);
			}
		} else {
			AliDip2BK.log(
					4,
					"ProcData.dispach",
					"!!!!!!!!!! Unimplemented Data Process for P=" + messageItem.param_name
			);
		}
	}

	public void newSafeMode(long time, int val) {

		if (currentFill == null) return;

		String bm = currentFill.getBeamMode();

		if (bm.contentEquals("STABLE BEAMS")) {

			boolean isB1 = BigInteger.valueOf(val).testBit(0);
			boolean isB2 = BigInteger.valueOf(val).testBit(4);
			boolean isSB = BigInteger.valueOf(val).testBit(2);

			AliDip2BK.log(
					0,
					"ProcData.newSafeBeams",
					" VAL=" + val + " isB1=" + isB1 + " isB2=" + isB2 + " isSB=" + isSB
			);
			if (isB1 && isB2) {
				return;
			} else {

				currentFill.setBeamMode(time, "LOST BEAMS");
				AliDip2BK.log(5, "ProcData.newSafeBeams", " CHANGE BEAM MODE TO LOST BEAMS !!! ");
			}

			return;
		}

		if (bm.contentEquals("LOST BEAMS")) {

			boolean isB1 = BigInteger.valueOf(val).testBit(0);
			boolean isB2 = BigInteger.valueOf(val).testBit(4);
			// boolean isSB = BigInteger.valueOf(val).testBit(2);

			if (isB1 && isB2) {
				currentFill.setBeamMode(time, "STABLE BEAMS");
				AliDip2BK.log(5, "ProcData.newSafeBeams", " RECOVER FROM BEAM LOST TO STABLE BEAMS ");
			}
		}
	}

	public RunInfoObj getRunNo(int runno) {
		if (ActiveRuns.size() == 0) {
			return null;
		}

		int k = -1;
		for (int i = 0; i < ActiveRuns.size(); i++) {
			RunInfoObj rio = ActiveRuns.get(i);
			if (rio.RunNo == runno) {
				k = i;
				break;
			}
		}
		if (k == -1) return null;
		RunInfoObj rio = ActiveRuns.get(k);
		return rio;
	}

	public void EndRun(RunInfoObj r1) {

		int k = -1;
		for (int i = 0; i < ActiveRuns.size(); i++) {
			RunInfoObj rio = ActiveRuns.get(i);
			if (rio.RunNo == r1.RunNo) {
				k = i;
				break;
			}
		}
		if (k == -1) {
			AliDip2BK.log(4, "ProcData.EndRun", " ERROR RunNo=" + r1.RunNo + " is not in the ACTIVE LIST ");
			statNoDuplicateEndRuns = statNoDuplicateEndRuns + 1;
			return;
		} else {

			statNoEndRuns = statNoEndRuns + 1;

			if (AliDip2BK.KEEP_RUNS_HISTORY_DIRECTORY != null) writeRunHistFile(r1);

			if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {

				if (r1.energyHist.size() >= 1) {
					String fn = "Energy_" + r1.RunNo + ".txt";
					writeHistFile(fn, r1.energyHist);
				}

				if (r1.l3_magnetHist.size() >= 1) {
					String fn = "L3magnet_" + r1.RunNo + ".txt";
					writeHistFile(fn, r1.l3_magnetHist);
				}
			}

			ActiveRuns.remove(k);
			String runList1 = "";
			if (ActiveRuns.size() > 0) {
				String runList = "[";
				for (int l = 0; l < ActiveRuns.size(); l++) {
					RunInfoObj rr = ActiveRuns.get(l);
					runList = runList + " " + rr.RunNo + ",";
				}
				runList1 = runList.substring(0, runList.length() - 1) + " ]";
			}

			AliDip2BK.log(
					2,
					"ProcData.EndRun",
					" Correctly closed  runNo=" + r1.RunNo + "  ActiveRuns size=" + ActiveRuns.size() + " " + runList1
			);

			if (r1.LHC_info_start.fillNo != r1.LHC_info_stop.fillNo) {

				AliDip2BK.log(
						5,
						"ProcData.EndRun",
						" !!!! RUN =" + r1.RunNo + "  Statred FillNo=" + r1.LHC_info_start.fillNo
								+ " and STOPED with FillNo=" + r1.LHC_info_stop.fillNo
				);
			}
		}
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
			statNoNewFills = statNoNewFills + 1;
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
					" Received new FILL no=" + no + "  BUT is an active FILL =" + currentFill.fillNo
							+ " Close the old one and created the new one"
			);
			currentFill.endedTime = (new Date()).getTime();
			if (AliDip2BK.KEEP_FILLS_HISTORY_DIRECTORY != null) {
				writeFillHistFile(currentFill);
			}
			bookkeepingClient.updateLhcFill(currentFill);

			currentFill = null;
			currentFill = new LhcInfoObj(date, no, par1, par2, ais, ip2Col, nob);
			bookkeepingClient.createLhcFill(currentFill);
			statNoNewFills = statNoNewFills + 1;
			saveState();
		}
	}

	public void newBeamMode(long date, String BeamMode) {

		if (currentFill != null) {
			currentFill.setBeamMode(date, BeamMode);

			int mc = -1;
			for (int i = 0; i < AliDip2BK.endFillCases.length; i++) {
				if (AliDip2BK.endFillCases[i].equalsIgnoreCase(BeamMode)) mc = i;
			}
			if (mc < 0) {

				AliDip2BK.log(
						2,
						"ProcData.newBeamMode",
						"New beam mode=" + BeamMode + "  for FILL_NO=" + currentFill.fillNo
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
						"CLOSE Fill_NO=" + currentFill.fillNo + " Based on new  beam mode=" + BeamMode
				);
				currentFill = null;
			}
		} else {
			AliDip2BK.log(4, "ProcData.newBeamMode", " ERROR new beam mode=" + BeamMode + " NO FILL NO for it");
		}
	}

	public void newEnergy(long time, float v) {

		if (currentFill != null) {
			currentFill.setEnergy(time, v);
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			if (ActiveRuns.size() == 0) return;

			for (int i = 0; i < ActiveRuns.size(); i++) {
				RunInfoObj r1 = ActiveRuns.get(i);
				r1.addEnergy(time, v);
			}
		}
	}

	public void newL3magnetCurrent(long time, float v) {

		if (currentAlice != null) {
			currentAlice.L3_magnetCurrent = v;
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			if (ActiveRuns.size() == 0) return;

			for (int i = 0; i < ActiveRuns.size(); i++) {
				RunInfoObj r1 = ActiveRuns.get(i);
				r1.addL3_magnet(time, v);
			}
		}
	}

	public void newDipoleCurrent(long time, float v) {

		if (currentAlice != null) {
			currentAlice.Dipole_magnetCurrent = v;
		}

		if (AliDip2BK.SAVE_PARAMETERS_HISTORY_PER_RUN) {
			if (ActiveRuns.size() == 0) return;

			for (int i = 0; i < ActiveRuns.size(); i++) {
				RunInfoObj r1 = ActiveRuns.get(i);
				r1.addDipoleMagnet(time, v);
			}
		}
	}

	public void newBetaStar(long t, float v) {

		if (currentFill != null) {
			currentFill.setLHCBetaStar(t, v);
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

	public void writeRunHistFile(RunInfoObj run) {
		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + AliDip2BK.KEEP_RUNS_HISTORY_DIRECTORY + "/run_" + run.RunNo + ".txt";

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

	public void writeHistFile(String filename, ArrayList<TimestampedFloat> A) {

		String path = getClass().getClassLoader().getResource(".").getPath();
		String full_file = path + AliDip2BK.STORE_HIST_FILE_DIR + "/" + filename;

		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));

			for (int i = 0; i < A.size(); i++) {
				TimestampedFloat ts = A.get(i);

				writer.write(ts.time + "," + ts.value + "\n");
			}
			writer.close();
		} catch (IOException e) {

			AliDip2BK.log(4, "ProcData.writeHistFile", " ERROR writing file=" + filename + "   ex=" + e);
		}
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

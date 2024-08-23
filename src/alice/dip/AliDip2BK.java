/*************
 * cil
 **************/

/*
 *  Main Class
 *
 */

package alice.dip;

import alice.dip.configuration.ApplicationConfiguration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class AliDip2BK implements Runnable {
	public static final SimpleDateFormat PERSISTENCE_DATE_FORMAT = new SimpleDateFormat("dd-MM-yy HH:mm");
	public static final SimpleDateFormat LOGGING_DATE_FORMAT = new SimpleDateFormat("dd-MM HH:mm:ss");

	// -- Garbage --
	public static int DEBUG_LEVEL = 1;

	private static final String VERSION = "2.0  14-Nov-2023";
	private static final String CONFIGURATION_FILE = "AliDip2BK.properties";

	private final String programPath;
	private final long startDate;

	// Configuration and managers

	private final ApplicationConfiguration configuration;

	private final DipClient client;
	private final DipMessagesProcessor dipMessagesProcessor;
	private final StartOfRunKafkaConsumer kcs;
	private final EndOfRunKafkaConsumer kce;
	private final StatisticsManager statisticsManager;
	private final FillManager fillManager;

	public AliDip2BK() {
		programPath = getClass().getClassLoader().getResource(".").getPath();
		startDate = (new Date()).getTime();

		this.configuration = parseConfigurationFile();

		showConfig();

		verifyDirs();

		statisticsManager = new StatisticsManager();
		var bookkeepingClient = new BookkeepingClient(configuration.bookkeepingClient());
		var runManager = new RunManager(configuration.persistence(), statisticsManager);
		fillManager = new FillManager(configuration.persistence(), bookkeepingClient, statisticsManager);
		fillManager.loadState();
		var aliceMagnetsManager = new AliceMagnetsManager();

		dipMessagesProcessor = new DipMessagesProcessor(
			configuration.persistence(),
			bookkeepingClient,
			runManager,
			fillManager,
			aliceMagnetsManager,
			statisticsManager
		);
		if (configuration.simulation().enabled()) {
			new SimDipEventsFill(fillManager);
		}

		client = new DipClient(configuration.dipClient(), dipMessagesProcessor);

		try {
			Thread.sleep(5000);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}

		kcs = new StartOfRunKafkaConsumer(configuration.kafkaClient(), dipMessagesProcessor);

		kce = new EndOfRunKafkaConsumer(configuration.kafkaClient(), dipMessagesProcessor);

		shutdownProc();

		Thread t = new Thread(this);
		t.start();
	}

	static public void log(int level, String module, String mess) {
		if (level >= DEBUG_LEVEL) {
			String date = LOGGING_DATE_FORMAT.format((new Date()).getTime());

			System.out.println("#" + level + " [" + date + "] " + module + " =>" + mess);
		}
	}

	public static void main(String[] args) {
		@SuppressWarnings("unused") AliDip2BK service = new AliDip2BK();
	}

	public void run() {
		int stat_count = 0;

		for (; ; ) {
			try {
				Thread.sleep(10000);
				stat_count = stat_count + 10;
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}

			if (stat_count >= 3600) {
				writeStat("StatHist.txt", false);
				stat_count = 0;
			}
		}
	}

	public void shutdownProc() {
		Runtime r = Runtime.getRuntime();
		r.addShutdownHook(new Thread(() -> {
			log(4, "AliDip2BK", "Main class  ENTERS in Shutdown hook");
			client.closeSubscriptions();
			dipMessagesProcessor.closeInputQueue();
			if (dipMessagesProcessor.queueSize() > 0) {
				for (int i = 0; i < 5; i++) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}

					if (dipMessagesProcessor.queueSize() == 0) break;
				}
			}

			if (dipMessagesProcessor.queueSize() != 0) {
				log(4, "AliDip2BK Shutdown", " Data Proc queue is not EMPTY ! Close it anyway ");
			} else {
				log(2, "AliDip2BK Shutdown", " Data Proc queue is EMPTY and it was correctly closed  ");
			}
			fillManager.saveState();
			writeStat("AliDip2BK.stat", true);
		}));
	}

	public void showConfig() {
		String con = "*************************************************\n";

		con = con + "* \n";
		con = con + "* AkiDip2BK Version =" + VERSION + "\n";
		con = con + "* DIP/DIM =" + this.configuration.dipClient().dnsNode() + "\n";
		con = con + "* KAFKA Server = " + this.configuration.kafkaClient().bootstrapServers() + "\n";
		con = con + "* KAFKA Group ID=" + this.configuration.kafkaClient().groupId() + "\n";
		con = con + "* Bookkeeping URL =" + this.configuration.bookkeepingClient().url() + "\n";
		con = con + "* \n";
		con = con + "*************************************************\n";

		System.out.println(con);
	}

	private ApplicationConfiguration parseConfigurationFile() {
		String input = programPath + "/" + CONFIGURATION_FILE;

		Properties properties = new Properties();

		try {
			properties.load(new FileInputStream(input));
		} catch (IOException ex) {
			log(4, "AliDip2BK.loadCong", "Failed to access properties file " + ex);
		}

		return ApplicationConfiguration.parseProperties(properties);
	}

	public void writeStat(String file, boolean finalReport) {
		String full_file = programPath + configuration.persistence().applicationStateDirectory() + file;

		var stopDate = (new Date()).getTime();
		double dur = (double) (stopDate - startDate) / (1000 * 60 * 60);

		Runtime rt = Runtime.getRuntime();
		long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

		String mess = "\n\n AliDip2BK Statistics \n";
		mess = mess + " Started :" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(startDate) + "\n";
		if (finalReport) {
			mess = mess + " Stopped :" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(stopDate) + "\n";
		}
		mess = mess + " Duration [h]=" + dur + "\n";
		mess = mess + " Memory Used [MB]=" + usedMB + "\n";
		mess = mess + " No of DIP messages=" + statisticsManager.getDipMessagesCount() + "\n";
		mess = mess + " No of KAFKA  messages=" + statisticsManager.getKafkaMessagesCount() + "\n";
		mess = mess + " No of KAFKA SOR messages=" + kcs.NoMess + "\n";
		mess = mess + " No of KAFKA EOR messages=" + kce.NoMess + "\n";
		mess = mess + " No of new Fill messgaes =" + statisticsManager.getNewFillsCount() + "\n";
		mess = mess + " No of new Run messgaes =" + statisticsManager.getNewRunsCount() + "\n";
		mess = mess + " No of end Run messages =" + statisticsManager.getEndedRunsCount() + "\n";
		mess = mess + " No of Duplicated end Run messages =" + statisticsManager.getDuplicatedRunsEndCount() + "\n";

		try {
			File of = new File(full_file);
			if (!of.exists()) {
				of.createNewFile();
			}
			BufferedWriter writer = new BufferedWriter(new FileWriter(full_file, true));

			writer.write(mess);
			writer.close();
		} catch (IOException e) {

			AliDip2BK.log(4, "ProcData.writeStat", " ERROR writing file=" + full_file + "   ex=" + e);
		}
	}

	public void verifyDirs() {
		verifyDir(configuration.persistence().runsHistoryDirectory());
		verifyDir(configuration.persistence().fillsHistoryDirectory());
		verifyDir(configuration.persistence().parametersHistoryDirectory());
		verifyDir(configuration.persistence().applicationStateDirectory());
	}

	public void verifyDir(String name) {
		if (name != null) {

			File directory = new File(programPath + "/" + name);

			if (!directory.exists()) {
				directory.mkdir();
				AliDip2BK.log(2, "AliDip2BK->verifyDir", "created new Directory=" + name);
			}
		}
	}
}


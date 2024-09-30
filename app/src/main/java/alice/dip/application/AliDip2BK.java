/*************
 * cil
 **************/

/*
 *  Main Class
 *
 */

package alice.dip.application;

import alice.dip.dipclient.DipClient;
import alice.dip.dipclient.DipMessagesProcessor;
import alice.dip.simulation.SimDipEventsFill;
import alice.dip.bookkeeping.BookkeepingClient;
import alice.dip.bookkeeping.BookkeepingRunUpdatePayload;
import alice.dip.configuration.ApplicationConfiguration;
import alice.dip.core.*;
import alice.dip.kafka.EndOfRunKafkaConsumer;
import alice.dip.kafka.StartOfRunKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class AliDip2BK implements Runnable {
	public static final SimpleDateFormat PERSISTENCE_DATE_FORMAT = new SimpleDateFormat("dd-MM-yy HH:mm");

	private static final String VERSION = "2.0  14-Nov-2023";
	private static final String CONFIGURATION_FILE = "AliDip2BK.properties";

	private final long startDate;

	// Configuration and managers

	private final ApplicationConfiguration configuration;

	private final DipClient client;
	private final DipMessagesProcessor dipMessagesProcessor;
	private final StartOfRunKafkaConsumer kcs;
	private final EndOfRunKafkaConsumer kce;
	private final StatisticsManager statisticsManager;
	private final FillManager fillManager;

	private final Logger logger = LoggerFactory.getLogger(AliDip2BK.class);

	public AliDip2BK() {
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
		var luminosityManager = new LuminosityManager();

		dipMessagesProcessor = new DipMessagesProcessor(
			configuration.persistence(),
			runManager,
			fillManager,
			aliceMagnetsManager,
			statisticsManager,
			luminosityManager
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

		kcs = new StartOfRunKafkaConsumer(
			configuration.kafkaClient(),
			(date, runNumber) -> {
				statisticsManager.incrementKafkaMessagesCount();
				var newRun = runManager.handleNewRun(
					date,
					runNumber,
					fillManager.getCurrentFill().map(LhcInfoObj::getView).orElse(null),
					luminosityManager.getView(),
					aliceMagnetsManager.getView()
				);
				bookkeepingClient.updateRun(BookkeepingRunUpdatePayload.of(newRun));
			}
		);

		kce = new EndOfRunKafkaConsumer(
			configuration.kafkaClient(),
			(date, runNumber) -> {
				statisticsManager.incrementKafkaMessagesCount();
				var luminosityAtEnd = luminosityManager.getView();
				runManager.handleRunEnd(
					date,
					runNumber,
					fillManager.getCurrentFill().map(LhcInfoObj::getView).orElse(null),
					aliceMagnetsManager.getView(),
					luminosityAtEnd
				);
				var updateRunPayload = new BookkeepingRunUpdatePayload(runNumber);
				if (luminosityAtEnd.phaseShift().isPresent()) {
					updateRunPayload.setPhaseShiftAtEnd(luminosityAtEnd.phaseShift().get());
				}
				bookkeepingClient.updateRun(updateRunPayload);
			}
		);

		shutdownProc();

		Thread t = new Thread(this);
		t.start();
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
			logger.info("Main class ENTERS in Shutdown hook");
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
				logger.error("Data Proc queue is not EMPTY ! Close it anyway");
			} else {
				logger.info("Data Proc queue is EMPTY and it was correctly closed");
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
		Properties properties = new Properties();

		try (var propertiesStream = getClass().getClassLoader().getResourceAsStream(CONFIGURATION_FILE)) {
			if (propertiesStream != null) {
				properties.load(propertiesStream);
			} else {
				logger.warn("Properties file not found {}", CONFIGURATION_FILE);
			}
		} catch (IOException ex) {
			logger.error("Failed to access properties file", ex);
		}

		return ApplicationConfiguration.parseProperties(properties);
	}

	public void verifyDirs() {
		try {
			var runsHistoryPath = configuration.persistence().runsHistoryPath();
			if (runsHistoryPath.isPresent()) {
				Files.createDirectories(runsHistoryPath.get());
			}

			var fillsHistoryPath = configuration.persistence().fillsHistoryPath();
			if (fillsHistoryPath.isPresent()) {
				Files.createDirectories(fillsHistoryPath.get());
			}

			Files.createDirectories(configuration.persistence().parametersHistoryPath());
			Files.createDirectories(configuration.persistence().applicationStatePath());
		} catch (IOException e) {
			logger.error("ERROR preparing data directories", e);
		}
	}

	public void writeStat(String file, boolean finalReport) {
		var destinationPath = configuration.persistence().applicationStatePath().resolve(file);

		var stopDate = (new Date()).getTime();
		double dur = (double) (stopDate - startDate) / (1000 * 60 * 60);

		Runtime rt = Runtime.getRuntime();
		long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

		String mess = "\n\n AliDip2BK Statistics \n";
		mess = mess + " Started :" + PERSISTENCE_DATE_FORMAT.format(startDate) + "\n";
		if (finalReport) {
			mess = mess + " Stopped :" + PERSISTENCE_DATE_FORMAT.format(stopDate) + "\n";
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

		try (
			var writer = Files.newBufferedWriter(
				destinationPath,
				StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE
			)
		) {
			writer.write(mess);
		} catch (IOException e) {
			logger.error("ERROR writing file={}", destinationPath, e);
		}
	}
}


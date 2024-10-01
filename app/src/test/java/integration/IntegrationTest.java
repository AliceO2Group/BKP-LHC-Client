package integration;

import alice.dip.application.AliDip2BK;
import alice.dip.bookkeeping.BookkeepingClient;
import alice.dip.configuration.ApplicationConfiguration;
import alice.dip.configuration.BookkeepingClientConfiguration;
import alice.dip.configuration.PersistenceConfiguration;
import alice.dip.core.*;
import alice.dip.dipclient.DipMessagesProcessor;
import cern.dip.*;
import cern.dip.implementation.DipDataImp;
import cern.dip.implementation.DipPublicationImp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.http.HttpClient;
import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IntegrationTest {
	private static AutoCloseable closeableMockito;
	private DipMessagesProcessor dipMessagesProcessor;

	@Mock
	private HttpClient httpClient;

	@BeforeAll
	static void before() {
		closeableMockito = MockitoAnnotations.openMocks(IntegrationTest.class);
	}

	@AfterAll
	static void after() throws Exception {
		closeableMockito.close();
	}

	@BeforeEach
	void beforeEach() {
		var persistenceConfiguration = ApplicationConfiguration.getPersistenceConfiguration(
			Optional.empty(),
			Optional.empty(),
			false
		);
		var bookkeepingClientConfiguration = new BookkeepingClientConfiguration("http://localhost:4000", null);

		var runManager = new RunManager(persistenceConfiguration);
		var bookkeepingClient = new BookkeepingClient(bookkeepingClientConfiguration, httpClient);
		var fillManager = new FillManager(persistenceConfiguration, bookkeepingClient);
		var aliceMagnetsManager = new AliceMagnetsManager();
		var statisticsManager = new StatisticsManager();
		var luminosityManager = new LuminosityManager();

		dipMessagesProcessor = new DipMessagesProcessor(
			persistenceConfiguration,
			runManager,
			fillManager,
			aliceMagnetsManager,
			statisticsManager,
			luminosityManager
		);
	}

	@Test
	void test() throws TypeMismatch {

		var runConfigurationParameter = "dip/acc/LHC/RunControl/RunConfiguration";
		var safeBeamParameter = "dip/acc/LHC/RunControl/SafeBeam";
		var beamEnergyParameter = "dip/acc/LHC/Beam/Energy";
		var beamModeParameter = "dip/acc/LHC/RunControl/BeamMode";
		var betaStarParameter = "dip/acc/LHC/Beam/BetaStar/Bstar2";
		var solenoidCurrentParameter = "dip/ALICE/MCS/Solenoid/Current";
		var dipoleCurrentParameter = "dip/ALICE/MCS/Dipole/Current";
		var solenoidPolarityParameter = "dip/ALICE/MCS/Solenoid/Polarity";
		var dipolePolarityParameter = "dip/ALICE/MCS/Dipole/Polarity";
		var bookkeepingSourceParameter = "dip/ALICE/LHC/Bookkeeping/Source";
		var ctpClockParameter = "dip/ALICE/LHC/Bookkeeping/CTPClock";

		var runConfigurationDipData = new DummyDipData();
		runConfigurationDipData.insert("FILL_NO", "123");
		runConfigurationDipData.insert("PARTICLE_TYPE_B1", "B1ParticleType");
		runConfigurationDipData.insert("PARTICLE_TYPE_B2", "B2ParticleType");
		runConfigurationDipData.insert("ACTIVE_INJECTION_SCHEME", "STABLE BEAMS");
		runConfigurationDipData.insert("IP2-NO-COLLISIONS", "9876");
		runConfigurationDipData.insert("NO_BUNCHES", "6789");

		dipMessagesProcessor.handleMessage(runConfigurationParameter, runConfigurationDipData);
	}
}

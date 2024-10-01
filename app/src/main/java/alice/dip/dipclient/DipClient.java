/*************
 * cil
 **************/

// Dip Client 
// Subscribe to  Dip data providers defined in the DipParametersFile
// Send the received information to ProcData for creating the Fill and Run data structures
//

package alice.dip.dipclient;

import java.io.*;

import java.util.HashMap;
import java.util.Map;

import alice.dip.configuration.DipClientConfiguration;
import cern.dip.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DipClient implements Runnable {

	public int NoMess = 0;
	public boolean status = true;
	DipFactory dip;
	DipMessagesProcessor procData;
	HashMap<String, DipSubscription> SubscriptionMap = new HashMap<>();

	private final Logger logger = LoggerFactory.getLogger(DipClient.class);

	public DipClient(DipClientConfiguration configuration, DipMessagesProcessor procData) {

		readParamFile(configuration.parametersFileName());

		initDIP(configuration.dnsNode());

		this.procData = procData;

		Thread t = new Thread(this);
		t.start();
	}

	public void run() {
		for (; ; ) {
			try {
				Thread.sleep(10000);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public void initDIP(String dnsNode) {
		// initialize the DIP client

		dip = Dip.create();
		dip.setDNSNode(dnsNode);

		// Handles all data subscriptions
		GeneralDataListener handler = new GeneralDataListener();

		for (Map.Entry<String, DipSubscription> m : SubscriptionMap.entrySet()) {
			String k = m.getKey();

			try {
				DipSubscription subx = dip.createDipSubscription(k, handler);
				SubscriptionMap.put(k, subx);
			} catch (DipException e) {
				logger.error("Error creating new subscription for param={}", k, e);
			}
		}

		logger.debug("Subscribed to {} data providers", SubscriptionMap.size());
		status = true;
	}

	// Used when the programs stops
	public void closeSubscriptions() {

		for (Map.Entry<String, DipSubscription> m : SubscriptionMap.entrySet()) {
			String k = m.getKey();
			DipSubscription subx = m.getValue();

			try {
				dip.destroyDipSubscription(subx);
			} catch (DipException e) {
				logger.error("Error closing subscription for param={}", k, e);
			}
		}
		SubscriptionMap.clear();
		logger.debug("Successfully closed all DIP Subscriptions");
	}

	public void readParamFile(String file_name) {
		try (var paramFileInputStream = getClass().getClassLoader().getResourceAsStream(file_name)) {
			var subscriptionsCount = 0;
			if (paramFileInputStream != null) {
				var reader = new BufferedReader(new InputStreamReader(paramFileInputStream));
				String line;
				while ((line = reader.readLine()) != null) {
					String dipSubscription = line.trim();
					if (!dipSubscription.startsWith("#")) {
						if (!SubscriptionMap.containsKey(dipSubscription)) {
							SubscriptionMap.put(dipSubscription, null);
							subscriptionsCount++;
						} else {
							logger.warn(" DUPLICATE Parameter ={}", dipSubscription);
						}
					}
				}
			}

			logger.debug("Read configuration: number of parameters NP={}", subscriptionsCount);
		} catch (IOException e) {
			logger.error("ERROR reading parameter file={}", file_name, e);
		}
	}

	/**
	 * handler for connect/disconnect/data reception events
	 */
	class GeneralDataListener implements DipSubscriptionListener {
		private final Logger logger = LoggerFactory.getLogger(GeneralDataListener.class);

		/**
		 * handle changes to subscribed to publications
		 */
		public void handleMessage(DipSubscription subscription, DipData message) {

			String p_name = subscription.getTopicName();
			String ans = Util.parseDipMess(p_name, message);

			logger.debug("New Dip mess from {} {}", p_name, ans);

			NoMess = NoMess + 1;

			procData.handleMessage(p_name, message);
		}

		/**
		 * called when a publication subscribed to is available.
		 *
		 * @param arg0 - the subsctiption who's publication is available.
		 */
		public void connected(DipSubscription arg0) {
			logger.info("Connected to {}", arg0.getTopicName());
		}

		/**
		 * called when a publication subscribed to is unavailable.
		 *
		 * @param dipSubscription - the subscription whose publication is unavailable.
		 * @param reason          - string providing more information about why the publication is
		 *                        unavailable.
		 */
		public void disconnected(DipSubscription dipSubscription, String reason) {
			logger.info("Disconnected from {} {}", dipSubscription.getTopicName(), reason);
			status = false;
		}

		@Override
		public void handleException(DipSubscription dipSubscription, Exception e) {
			logger.error("ERROR subscription={}", dipSubscription.getTopicName(), e);
		}
	}
}

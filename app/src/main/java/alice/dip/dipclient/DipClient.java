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
import alice.dip.application.AliDip2BK;
import cern.dip.*;
//import cern.dip.dim.Dip;

public class DipClient implements Runnable {

	public int NoMess = 0;
	public boolean status = true;
	DipFactory dip;
	long MAX_TIME_TO_UPDATE = 60;// in s
	DipMessagesProcessor procData;
	HashMap<String, DipSubscription> SubscriptionMap = new HashMap<>();
	HashMap<String, DipData> DataMap = new HashMap<>();

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
				AliDip2BK.log(4, "DipClient.initDP", " error creating new subscription for param=" + k + " e=" + e);
			}
		}

		AliDip2BK.log(1, "DipClient.initDP", " Subscribed to " + SubscriptionMap.size() + " data provides");
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
				AliDip2BK.log(
					4,
					"DipClient.CloseSubscriptions",
					" error closing subscription for param=" + k + " e=" + e
				);
			}
		}
		SubscriptionMap.clear();
		AliDip2BK.log(1, "DipClient.CloseSubscriptions", " Succesfuly closed all DIP Subscriptions");
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
							AliDip2BK.log(3, "DipClient.readParam", " DUPLICATE Parameter =" + dipSubscription);
						}
					}
				}
			}

			AliDip2BK.log(1, "DipClient.readParam", " Configuration:  number of parameters NP=" + subscriptionsCount);
		} catch (IOException e) {
			AliDip2BK.log(4, "DipClient.readParam", " ERROR reading parameter file=" + file_name + "ex=" + e);
		}
	}

	/**
	 * handler for connect/disconnect/data reception events
	 */
	class GeneralDataListener implements DipSubscriptionListener {

		/**
		 * handle changes to subscribed to publications
		 */
		public void handleMessage(DipSubscription subscription, DipData message) {

			String p_name = subscription.getTopicName();
			String ans = Util.parseDipMess(p_name, message);

			AliDip2BK.log(0, "DipClient", " new Dip mess from " + p_name + " " + ans);

			NoMess = NoMess + 1;

			procData.handleMessage(p_name, ans, message);
		}

		/**
		 * called when a publication subscribed to is available.
		 *
		 * @param arg0 - the subsctiption who's publication is available.
		 */
		public void connected(DipSubscription arg0) {
			// AliDip2BK.log(3, "DpiClient.GeneralDataListener.connect", "Publication source
			// available "+arg0);
		}

		/**
		 * called when a publication subscribed to is unavailable.
		 *
		 * @param arg0 - the subsctiption who's publication is unavailable.
		 * @param arg1 - string providing more information about why the publication is
		 *             unavailable.
		 */
		public void disconnected(DipSubscription arg0, String arg1) {
			AliDip2BK.log(
				4,
				"DipClient.GeneralDataListener.disconnect",
				"Publication source unavailable " + arg0 + "  " + arg1
			);
			status = false;
		}

		@Override
		public void handleException(DipSubscription arg0, Exception arg1) {
			AliDip2BK.log(4, "DipClient.GeneralDataListener.Exception ", "Exception= " + arg0 + " arg1=" + arg1);
		}
	}
}

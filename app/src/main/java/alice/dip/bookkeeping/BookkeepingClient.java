/*************
 * cil
 **************/

/*
 * This class is used to write the Dip information into the
 * Bookkeeping Data Base
 */

package alice.dip.bookkeeping;

import alice.dip.core.LhcFillView;
import alice.dip.configuration.BookkeepingClientConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class BookkeepingClient {
	private final HttpClient httpClient;
	private final String bookkeepingUrl;
	private final String bookkeepingToken;

	private final Logger logger = LoggerFactory.getLogger(BookkeepingClient.class);

	public BookkeepingClient(BookkeepingClientConfiguration bookkeepingClientConfiguration, HttpClient httpClient) {
		this.httpClient = httpClient;

		this.bookkeepingUrl = bookkeepingClientConfiguration.url();
		this.bookkeepingToken = bookkeepingClientConfiguration.token();
	}

	public boolean doesFillExists(int fillNumber) {
		String getFillUrl = bookkeepingUrl + "/api/lhcFills/" + fillNumber;
		if (bookkeepingToken != null) {
			getFillUrl += "?token=" + bookkeepingToken;
		}

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(getFillUrl))
			.GET() // default
			.build();

		HttpResponse<String> response;

		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() == 200) {
				String prob = "\"fillNumber\":" + fillNumber;
				String body = response.body();

				return body.contains(prob);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	public void createLhcFill(LhcFillView lhcFillView) {
		boolean fillExists = doesFillExists(lhcFillView.fillNumber());

		if (fillExists) {
			logger.warn("INSERT FILL... BUT Fill No={} is in BK ... trying to update record", lhcFillView.fillNumber());
			updateLhcFill(lhcFillView);
			return;
		}

		String createLhcFillUrl = bookkeepingUrl + "/api/lhcFills";
		if (bookkeepingToken != null) {
			createLhcFillUrl += "?token=" + bookkeepingToken;
		}

		String requestBody = "{";
		requestBody = requestBody + "\n\"fillingSchemeName\":\"" + lhcFillView.fillingSchemeName() + "\",";
		requestBody = requestBody + "\n\"beamType\":\"" + lhcFillView.beamType() + "\",";
		requestBody = requestBody + "\n\"fillNumber\":" + lhcFillView.fillNumber() + ",";

		if (requestBody.endsWith(",")) {
			requestBody = requestBody.substring(0, requestBody.length() - 1);
		}
		requestBody = requestBody + "\n}";

		logger.debug("FILL INSERT JSON request=\n{}", requestBody);

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(createLhcFillUrl))
			.header("Content-Type", "application/json")
			.method("POST", HttpRequest.BodyPublishers.ofString(requestBody))
			.build();

		HttpResponse<String> response;

		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			logger.info("INSERT new FILL No={} Code={}", lhcFillView.fillNumber(), response.statusCode());
		} catch (Exception e) {
			logger.error("Insert fill HTTP ERROR", e);
		}
	}

	/*
	 *  This method is used when new updates are received on the current Fill
	 *  The modified values are updated in the DB
	 */
	public void updateLhcFill(LhcFillView lhcFillView) {
		boolean fillExists = doesFillExists(lhcFillView.fillNumber());

		if (!fillExists) {
			logger.error("Fill No={} is NOT in BK", lhcFillView.fillNumber());
			return;
		}

		String updateFillRequestBody = "{";

		var stableBeamStart = lhcFillView.stableBeamStart();
		if (stableBeamStart.isPresent()) {
			updateFillRequestBody += "\n\"stableBeamsStart\":" + stableBeamStart.get() + ",";
		}

		var stableBeamStop = lhcFillView.stableBeamStop();
		if (stableBeamStart.isPresent() && stableBeamStop.isPresent()) {
			updateFillRequestBody += "\n\"stableBeamsEnd\":" + stableBeamStop.get() + ",";
		}

		int stableBeamDuration = lhcFillView.stableBeamsDuration();
		if (stableBeamDuration > 0) {
			updateFillRequestBody += "\n\"stableBeamsDuration\":" + stableBeamDuration + ",";
		}

		updateFillRequestBody += "\n\"fillingSchemeName\":\"" + lhcFillView.fillingSchemeName() + "\",";

		if (updateFillRequestBody.endsWith(",")) {
			updateFillRequestBody = updateFillRequestBody.substring(0, updateFillRequestBody.length() - 1);
		}
		updateFillRequestBody = updateFillRequestBody + "\n}";

		logger.debug("UPDATE FILL={} JSON request=\n{}", lhcFillView.fillNumber(), updateFillRequestBody);

		String updateFillUrl = bookkeepingUrl + "/api/lhcFills/" + lhcFillView.fillNumber();
		if (bookkeepingToken != null) {
			updateFillUrl += "?token=" + bookkeepingToken;
		}

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(updateFillUrl))
			.header("Content-Type", "application/json")
			.method("PATCH", HttpRequest.BodyPublishers.ofString(updateFillRequestBody))
			.build();

		HttpResponse<String> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			if ((response.statusCode() == 201)) {
				logger.info("Successful Update for FILL={}", lhcFillView.fillNumber());
			} else {
				logger.warn(
					"ERROR updating FILL={} Code={} Body={}",
					lhcFillView.fillNumber(),
					response.statusCode(),
					response.body()
				);
			}
		} catch (Exception e) {
			logger.error("ERROR Update for FILL={}", lhcFillView.fillNumber(), e);
		}
	}

	public boolean doesRunExists(int runNumber) {
		// FIXME with the new filtering, fetching run number like 1234 will match if a run like `12345` exists
		String getRunsUrl = bookkeepingUrl + "/api/runs?filter[runNumbers]=" + runNumber;

		if (bookkeepingToken != null) {
			getRunsUrl += "&token=" + bookkeepingToken;
		}

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(getRunsUrl))
			.GET() // default
			.build();

		HttpResponse<String> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() == 200) {
				String prob = "\"runNumber\":" + runNumber;
				String body = response.body();

				return body.contains(prob);
			}

			logger.warn(
				"Request error checking if run exists Status={} Body={}",
				response.statusCode(),
				response.body()
			);
		} catch (Exception e) {
			logger.error("Error when checking if run exists", e);
		}

		return false;
	}

	/*
	 *  This method is used to update the RUN info entry
	 */
	public void updateRun(BookkeepingRunUpdatePayload runUpdatePayload) {
		if (runUpdatePayload.isEmpty()) {  // no updates to be done !
			logger.warn("No data to update for Run={}", runUpdatePayload.getRunNumber());
			return;
		}

		boolean runExists;
		int retriesCounter = 0;
		do {
			runExists = doesRunExists(runUpdatePayload.getRunNumber());
			if (!runExists) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		} while (++retriesCounter <= 10 && !runExists);

		if (retriesCounter > 0) logger.debug("DELAY BKP UPDATE REQUEST Loop Count={}", retriesCounter);

		String requestBody;
		try {
			requestBody = new ObjectMapper()
				.registerModule(new Jdk8Module())
				.writeValueAsString(runUpdatePayload);
		} catch (JsonProcessingException e) {
			logger.error("Error in serializing request body while updating run", e);
			return;
		}

		logger.debug("Update RUN ={} UPDATE JSON request=\n{}", runUpdatePayload.getRunNumber(), requestBody);

		String patchRunRequest = bookkeepingUrl + "/api/runs?runNumber=" + runUpdatePayload.getRunNumber();

		if (bookkeepingToken != null) {
			patchRunRequest += bookkeepingToken;
		}

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(patchRunRequest))
			.header("Content-Type", "application/json")
			.method("PATCH", HttpRequest.BodyPublishers.ofString(requestBody))
			.build();

		HttpResponse<String> response;
		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() == 200) {
				logger.info("Succesful Update for RUN={}", runUpdatePayload.getRunNumber());
			} else {
				logger.warn(
					"ERROR updating RUN={} Code={} Body={}",
					runUpdatePayload.getRunNumber(),
					response.statusCode(),
					response.body()
				);
			}
		} catch (Exception e) {
			logger.error("ERROR Update for RUN={}", runUpdatePayload.getRunNumber(), e);
		}
	}
}


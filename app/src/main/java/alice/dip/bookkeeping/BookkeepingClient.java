/*************
 * cil
 **************/

/*
 * This class is used to write the Dip information into the
 * Bookkeeping Data Base
 */

package alice.dip.bookkeeping;

import alice.dip.AliDip2BK;
import alice.dip.LhcFillView;
import alice.dip.LhcInfoObj;
import alice.dip.configuration.BookkeepingClientConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class BookkeepingClient {
	private final HttpClient httpClient;
	private final String bookkeepingUrl;
	private final String bookkeepingToken;

	public BookkeepingClient(BookkeepingClientConfiguration bookkeepingClientConfiguration) {
		httpClient = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_2)
			.connectTimeout(Duration.ofSeconds(10))
			.build();

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
			AliDip2BK.log(
				3,
				"BKwriter.InserFill",
				"INSERT FILL ... BUT Fill No=" + lhcFillView.fillNumber() + " is in BK ... trying to update record"
			);
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

		AliDip2BK.log(1, "BKwriter.InserFill", "FILL INSERT JSON request=\n" + requestBody);

		HttpRequest request = HttpRequest.newBuilder()
			.uri(URI.create(createLhcFillUrl))
			.header("Content-Type", "application/json")
			.method("POST", HttpRequest.BodyPublishers.ofString(requestBody))
			.build();

		HttpResponse<String> response;

		try {
			response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			AliDip2BK.log(
				2,
				"BKwriter.InserFill",
				" INSERT new FILL No=" + lhcFillView.fillNumber() + "  Code=" + response.statusCode()
			);
		} catch (Exception e) {
			AliDip2BK.log(4, "BKwriter.InserFill", "HTTP ERROR=" + e);
			e.printStackTrace();
		}
	}

	/*
	 *  This method is used when new updates are received on the current Fill
	 *  The modified values are updated in the DB
	 */
	public void updateLhcFill(LhcFillView lhcFillView) {
		boolean fillExists = doesFillExists(lhcFillView.fillNumber());

		if (!fillExists) {
			AliDip2BK.log(4, "BKwriter.UPdate FILL", "Fill No=" + lhcFillView.fillNumber() + " is NOT in BK ");
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

		AliDip2BK.log(
			1,
			"BKwriter.UpdateFILL",
			"UPDATE FILL=" + lhcFillView.fillNumber() + " JSON request=\n" + updateFillRequestBody
		);

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
				AliDip2BK.log(2, "BKwriter.UpdateFILL", "Succesful Update for FILL=" + lhcFillView.fillNumber());
			} else {
				AliDip2BK.log(
					3,
					"BKwriter.UpdateFILL",
					"ERROR for FILL=" + lhcFillView.fillNumber() + " Code=" + +response.statusCode() + " Message="
						+ response.body()
				);
			}
		} catch (Exception e) {
			AliDip2BK.log(4, "BKwriter.UpdateFILL", "ERROR Update for FILL=" + lhcFillView.fillNumber() + "\n Exception=" + e);
			e.printStackTrace();
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

			AliDip2BK.log(
				3,
				"BKwriter.TestRunNo",
				" Reguest error =" + response.statusCode() + " Mesage=" + response.body()
			);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	/*
	 *  This method is used to update the RUN info entry
	 */
	public void updateRun(BookkeepingRunUpdatePayload runUpdatePayload) {
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

		if (retriesCounter > 0) AliDip2BK.log(1, "BKwriter.UpdateRun", "DELAY Loop Count=" + (retriesCounter));

		if (runUpdatePayload.isEmpty()) {  // no updates to be done !
			AliDip2BK.log(3, "BKwriter.UpdateRun", "No data to update for Run=" + runUpdatePayload.getRunNumber());
			return;
		}

		String requestBody;
		try {
			requestBody = new ObjectMapper()
				.registerModule(new Jdk8Module())
				.writeValueAsString(runUpdatePayload);
		} catch (JsonProcessingException e) {
			AliDip2BK.log(4, "BKwriter.UpdateRun", "Error in serializing request body");
			return;
		}

		AliDip2BK.log(1, "BKwriter.UpdateRun", "RUN =" + runUpdatePayload.getRunNumber() + " UPDATE JSON request=\n" + requestBody);

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
				AliDip2BK.log(2, "BKwriter.UpdateRun", "Succesful Update for RUN=" + runUpdatePayload.getRunNumber());
			} else {
				AliDip2BK.log(
					3,
					"BKwriter.UpdateRun",
					"ERROR for RUN=" + runUpdatePayload.getRunNumber() + " Code=" + +response.statusCode() + " Message=" + response.body()
				);
			}
		} catch (Exception e) {
			AliDip2BK.log(4, "BKwriter.UpdateRun", "ERROR Update for RUN=" + runUpdatePayload.getRunNumber() + "\n Exception=" + e);
			e.printStackTrace();
		}
	}
}


/*************
 * cil
 **************/
/*
 * Keeps the required LHC information
 */
package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LhcInfoObj implements Serializable {
	private final PersistenceConfiguration persistenceConfiguration;

	private static final long serialVersionUID = 1L;

	static String stableBeamName = "STABLE BEAMS";

	public int fillNo;
	public long createdTime = -1;
	public long endedTime = -1;
	public String Beam1ParticleType;
	public String Beam2ParticleType;
	public String beamType;
	public int LHCTotalInteractingBunches;
	public int LHCTotalNonInteractingBuchesBeam1;
	public int LHCTotalNonInteractingBuchesBeam2;
	public String LHCFillingSchemeName;
	public int IP2_NO_COLLISIONS;
	public int NO_BUNCHES;
	public ArrayList<TimestampedFloat> beamEnergyHist;
	public ArrayList<TimestampedFloat> LHCBetaStarHist;
	public ArrayList<TimestampedString> beamModeHist;
	public ArrayList<TimestampedString> fillingSchemeHist;
	public ArrayList<TimestampedString> ActiveFillingSchemeHist;
	private float beamEnergy;
	private float LHCBetaStar;

	public LhcInfoObj(
		PersistenceConfiguration persistenceConfiguration,
		long date,
		int fillNumber,
		String beam1ParticleType,
		String beam2ParticleType,
		String fillingScheme,
		int ip2CollisionsCount,
		int bunchesCount
	) {
		this.persistenceConfiguration = persistenceConfiguration;

		createdTime = date;
		fillNo = fillNumber;

		Beam1ParticleType = beam1ParticleType;
		Beam2ParticleType = beam2ParticleType;
		beamType = beam1ParticleType + " - " + beam2ParticleType;
		LHCFillingSchemeName = fillingScheme;
		IP2_NO_COLLISIONS = ip2CollisionsCount;
		NO_BUNCHES = bunchesCount;

		beamModeHist = new ArrayList<>();
		fillingSchemeHist = new ArrayList<>();
		beamEnergyHist = new ArrayList<>();
		LHCBetaStarHist = new ArrayList<>();
		ActiveFillingSchemeHist = new ArrayList<>();

		endedTime = -1;
		beamEnergy = -1;
		LHCTotalInteractingBunches = IP2_NO_COLLISIONS;
		LHCTotalNonInteractingBuchesBeam1 = NO_BUNCHES - IP2_NO_COLLISIONS;

		if (LHCTotalNonInteractingBuchesBeam1 < 0)
			LHCTotalNonInteractingBuchesBeam1 = 0;
		LHCTotalNonInteractingBuchesBeam2 = LHCTotalNonInteractingBuchesBeam1;
		LHCBetaStar = -1;

		TimestampedString ts1 = new TimestampedString(date, fillingScheme + " *");
		fillingSchemeHist.add(ts1);
	}

	public String toString() {
		String ans = " FILL No=" + fillNo + " StartTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(createdTime);
		if (endedTime > 0) {
			ans = ans + " EndTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(endedTime);
		}
		// ans = ans + " Beam1_ParticleType="+ Beam1ParticleType + "
		// Beam2_ParticleType="+ Beam2ParticleType;
		ans = ans + " Beam Mode=" + getBeamMode();
		ans = ans + " Beam Type=" + beamType;
		ans = ans + " LHC Filling Scheme =" + LHCFillingSchemeName;
		ans = ans + " Beam  Energy=" + beamEnergy + " Beta Star=" + LHCBetaStar;
		ans = ans + " LHCTotalInteractingBunches =" + LHCTotalInteractingBunches + " LHCTotalNonInteractingBuchesBeam1="
			+ LHCTotalNonInteractingBuchesBeam1;
		ans = ans + " Stable Beam Duration=" + getStableBeamDuration();
		return ans;
	}

	public String history() {
		String ans = " FILL No=" + fillNo + " StartTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(createdTime);
		if (endedTime > 0) {
			ans = ans + " EndTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(endedTime) + "\n";
		}
		ans = ans + " Beam1_ParticleType=" + Beam1ParticleType + " Beam2_ParticleType=" + Beam2ParticleType;
		ans = ans + " Beam Type=" + beamType;
		ans = ans + " LHC Filling Scheme =" + LHCFillingSchemeName + "\n";
		ans = ans + " Beam Energy=" + beamEnergy + " Beta Star=" + LHCBetaStar + "\n";
		ans = ans + " No_BUNCHES=" + NO_BUNCHES + "\n";
		ans = ans + " LHCTotalInteractingBunches =" + LHCTotalInteractingBunches + " LHCTotalNonInteractingBuchesBeam1="
			+ LHCTotalNonInteractingBuchesBeam1 + "\n";
		ans = ans + " Start Stable Beams =" + getStableBeamStartStr();
		ans = ans + " Stop Stable Beams =" + getStableBeamStopStr() + "\n";
		ans = ans + " Stable Beam Duration [s] =" + getStableBeamDuration() + "\n";

		if (beamModeHist.size() >= 1) {
			ans = ans + " History:: Beam Mode\n";

			for (int i = 0; i < beamModeHist.size(); i++) {
				TimestampedString a1 = beamModeHist.get(i);
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		if (fillingSchemeHist.size() >= 1) {
			ans = ans + " History:: Filling Scheme \n";

			for (int i = 0; i < fillingSchemeHist.size(); i++) {
				TimestampedString a1 = fillingSchemeHist.get(i);
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		if (ActiveFillingSchemeHist.size() >= 1) {
			ans = ans + " History:: Active Filling Scheme \n";

			for (int i = 0; i < ActiveFillingSchemeHist.size(); i++) {
				TimestampedString a1 = ActiveFillingSchemeHist.get(i);
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		if (beamEnergyHist.size() >= 1) {
			ans = ans + " History:: Beam Energy\n";

			for (int i = 0; i < beamEnergyHist.size(); i++) {
				TimestampedFloat a1 = beamEnergyHist.get(i);
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
			}
		}

		if (LHCBetaStarHist.size() >= 1) {
			ans = ans + " History:: LHC Beta Star\n";

			for (int i = 0; i < LHCBetaStarHist.size(); i++) {
				TimestampedFloat a1 = LHCBetaStarHist.get(i);
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
			}
		}

		return ans;
	}

	public LhcInfoObj clone() {
		LhcInfoObj n = new LhcInfoObj(
			persistenceConfiguration,
			createdTime,
			fillNo,
			Beam1ParticleType,
			Beam2ParticleType,
			LHCFillingSchemeName,
			IP2_NO_COLLISIONS,
			NO_BUNCHES
		);

		@SuppressWarnings("unchecked")
		ArrayList<TimestampedString> bmh = (ArrayList<TimestampedString>) beamModeHist.clone();
		@SuppressWarnings("unchecked")
		ArrayList<TimestampedString> fsh = (ArrayList<TimestampedString>) fillingSchemeHist.clone();
		@SuppressWarnings("unchecked")
		ArrayList<TimestampedFloat> eh = (ArrayList<TimestampedFloat>) beamEnergyHist.clone();
		@SuppressWarnings("unchecked")
		ArrayList<TimestampedFloat> bsh = (ArrayList<TimestampedFloat>) LHCBetaStarHist.clone();
		@SuppressWarnings("unchecked")
		ArrayList<TimestampedString> afsh = (ArrayList<TimestampedString>) ActiveFillingSchemeHist.clone();

		n.beamModeHist = bmh;
		n.fillingSchemeHist = fsh;
		n.ActiveFillingSchemeHist = afsh;
		n.beamEnergyHist = eh;
		n.LHCBetaStarHist = bsh;

		n.endedTime = endedTime;

		n.beamEnergy = beamEnergy;
		n.LHCTotalInteractingBunches = LHCTotalInteractingBunches;
		n.LHCTotalNonInteractingBuchesBeam1 = LHCTotalNonInteractingBuchesBeam1;
		n.LHCTotalNonInteractingBuchesBeam2 = LHCTotalNonInteractingBuchesBeam2;
		n.LHCBetaStar = LHCBetaStar;

		return n;
	}

	// Todo : this function should be in FillManager, its return is very case-specific (whether or not we notify bkp)
	public boolean verifyAndUpdate(long time, String fillingScheme, int ip2c, int nob) {
		boolean isBeamPhysicsInjection = false;
		boolean update = false;

		if (!fillingScheme.contentEquals(LHCFillingSchemeName)) {
			AliDip2BK.log(4, "LHCInfo.verify",
				"FILL=" + fillNo + "  Filling Scheme is different OLD=" + LHCFillingSchemeName + " NEW=" + fillingScheme
			);

			String beamMode = getBeamMode();
			if (beamMode != null) {
				if (beamMode.contains("INJECTION") && beamMode.contains("PHYSICS")) {
					isBeamPhysicsInjection = true;
					LHCFillingSchemeName = fillingScheme;
					IP2_NO_COLLISIONS = ip2c;
					NO_BUNCHES = nob;
					update = true;
					AliDip2BK.log(5, "LHCInfo.verify",
						"FILL=" + fillNo + " is IPB-> Changed Filling Scheme to :" + LHCFillingSchemeName
					);
				} else {
					AliDip2BK.log(4, "LHCInfo.verify",
						"FILL=" + fillNo + " is NOT in IPB keepFilling scheme to: " + LHCFillingSchemeName
					);
				}
			}

			saveFillingSchemeInHistory(time, fillingScheme, isBeamPhysicsInjection);
		}

		if (ip2c != IP2_NO_COLLISIONS) {
			AliDip2BK.log(4, "LHCInfo.verify",
				" FILL=" + fillNo + " IP2 COLLis different OLD=" + IP2_NO_COLLISIONS + " new=" + ip2c
			);
			IP2_NO_COLLISIONS = ip2c;
		}

		if (nob != NO_BUNCHES) {
			AliDip2BK.log(4, "LHCInfo.verify",
				" FILL=" + fillNo + " INO_BUNCHES is different OLD=" + NO_BUNCHES + " new=" + nob
			);
			NO_BUNCHES = nob;
		}

		return update;
	}

	public void addNewAFS(long time, String fs) {

		TimestampedString ts2 = new TimestampedString(time, fs);
		ActiveFillingSchemeHist.add(ts2);
	}

	private void saveFillingSchemeInHistory(long time, String fillingScheme, boolean isBeamPhysicsInjection) {
		TimestampedString ts2 = new TimestampedString(
			time,
			isBeamPhysicsInjection ? fillingScheme + " *" : fillingScheme
		);
		fillingSchemeHist.add(ts2);
	}

	public void setBeamMode(long date, String mode) {
		TimestampedString nv = new TimestampedString(date, mode);
		beamModeHist.add(nv);
	}

	public float getEnergy() {
		return beamEnergy;
	}

	public float getLHCBetaStar() {
		return LHCBetaStar;
	}

	public void setEnergy(long date, float v) {

		if (beamEnergyHist.isEmpty()) {
			TimestampedFloat v1 = new TimestampedFloat(date, v);
			beamEnergyHist.add(v1);
			beamEnergy = v;
			return;
		}

		double re = Math.abs(beamEnergy - v);
		if (re >= persistenceConfiguration.minimumLoggedEnergyDelta()) {
			TimestampedFloat v1 = new TimestampedFloat(date, v);
			beamEnergyHist.add(v1);
			beamEnergy = v;
		}
	}

	public void setLHCBetaStar(long date, float v) {
		if (LHCBetaStarHist.isEmpty()) {
			TimestampedFloat v1 = new TimestampedFloat(date, v);
			LHCBetaStarHist.add(v1);
			LHCBetaStar = v;
			return;
		}

		double re = Math.abs(LHCBetaStar - v);
		if (re >= persistenceConfiguration.minimumLoggedBetaDelta()) {
			TimestampedFloat v1 = new TimestampedFloat(date, v);
			LHCBetaStarHist.add(v1);
			LHCBetaStar = v;
		}
	}

	public String getBeamMode() {
		if (beamModeHist.isEmpty()) {
			return null;
		}
		TimestampedString last = beamModeHist.get(beamModeHist.size() - 1);
		return last.value;
	}

	public String getStableBeamStartStr() {

		long t = getStableBeamStart();
		if (t < 0) {
			return "No Stable Beam";
		} else {
			return AliDip2BK.PERSISTENCE_DATE_FORMAT.format(t);
		}
	}

	public String getStableBeamStopStr() {

		long t = getStableBeamStop();
		if (t < 0) {
			return "No Stable Beam";
		} else {
			return AliDip2BK.PERSISTENCE_DATE_FORMAT.format(t);
		}
	}

	public long getStableBeamStart() {
		long ans = -1;

		for (int i = 0; i < beamModeHist.size(); i++) {

			TimestampedString a1 = beamModeHist.get(i);

			if (a1.value.equalsIgnoreCase(stableBeamName)) {
				ans = a1.time;
				break;
			}
		}

		return ans;
	}

	public long getStableBeamStop() {

		// return -1 is not define
		// return 0 if in stable beams
		//
		long sbs = getStableBeamStart();
		if (sbs == -1) {
			return -1;
		}
		int idx = -1;

		for (int i = beamModeHist.size() - 1; i >= 0; i--) {

			TimestampedString a1 = beamModeHist.get(i);
			if (a1.value.equalsIgnoreCase(stableBeamName)) {
				idx = i;
				break;
			}
		}

		if (idx < 0) {
			return -1;
		}
		if (idx == (beamModeHist.size() - 1)) { // last entry
			if (endedTime < 0) { // fill is still active
				return 0; // going on
			} else {
				return endedTime;
			}
		}
		TimestampedString a2 = beamModeHist.get(idx + 1);

		long sbstop = a2.time;

		return sbstop;
	}

	public int getStableBeamDuration() {
		long sum = 0;

		if (beamModeHist.size() == 0)
			return 0;

		if (beamModeHist.size() == 1) {
			TimestampedString a1 = beamModeHist.get(0);
			if (a1.value.equalsIgnoreCase(stableBeamName) && (endedTime > 0)) {
				long dt = endedTime - a1.time;
				int ans = (int) (dt / 1000);
				return ans;
			} else if (a1.value.equalsIgnoreCase(stableBeamName) && (endedTime < 0)) {
				long now = (new Date()).getTime();
				long dt = now - a1.time;
				int ans = (int) (dt / 1000);
				return ans;
			} else {
				return 0;
			}
		}

		for (int i = 0; i < (beamModeHist.size() - 1); i++) {

			TimestampedString a1 = beamModeHist.get(i);
			TimestampedString a2 = beamModeHist.get(i + 1);
			if (a1.value.equalsIgnoreCase(stableBeamName)) {
				sum = sum + (a2.time - a1.time);
			}
		}

		TimestampedString a3 = beamModeHist.get(beamModeHist.size() - 1); // last entry

		if (a3.value.equalsIgnoreCase(stableBeamName) && (endedTime > 0)) {

			sum = sum + (endedTime - a3.time);
		}

		if (a3.value.equalsIgnoreCase(stableBeamName) && (endedTime == -1)) {
			long now = (new Date()).getTime();

			sum = sum + (now - a3.time);
		}

		int ans = (int) (sum / 1000);
		return ans;
	}
}
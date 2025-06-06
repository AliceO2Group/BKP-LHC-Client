/*************
 * cil
 **************/
/*
 *  Keeps the RUN Information
 */
package alice.dip;

import java.util.ArrayList;
import java.util.Optional;

public class RunInfoObj {

	public int RunNo;
	public LhcInfoObj LHC_info_start;
	public LhcInfoObj LHC_info_stop;
	public AliceInfoObj alice_info_start;
	public AliceInfoObj alice_info_stop;
	public ArrayList<TimestampedFloat> energyHist;
	public ArrayList<TimestampedFloat> l3_magnetHist;
	public ArrayList<TimestampedFloat> DipoleMagnetHist;
	public long SOR_time;
	public long EOR_time;
	public final LuminosityView luminosityAtStart;
	public Optional<LuminosityView> luminosityAtStop = Optional.empty();

	public RunInfoObj(
			long sor_time,
			int RunNo,
			LhcInfoObj start,
			AliceInfoObj alice_start,
			LuminosityView luminosityAtStart
	) {
		this.RunNo = RunNo;
		SOR_time = sor_time;
		this.LHC_info_start = start;
		this.alice_info_start = alice_start;
		energyHist = new ArrayList<TimestampedFloat>();
		l3_magnetHist = new ArrayList<TimestampedFloat>();
		DipoleMagnetHist = new ArrayList<TimestampedFloat>();
		this.luminosityAtStart = luminosityAtStart;
	}

	public String toString() {

		String ans = "RUN=" + RunNo + "\n";
		ans = ans + "SOR=" + AliDip2BK.myDateFormat.format(SOR_time) + "\n";
		if (LHC_info_start != null) {
			ans = ans + "LHC:: " + LHC_info_start.toString() + "\n";
		} else {
			ans = ans + "LHC:: No DATA \n";
		}
		if (alice_info_start != null) {
			ans = ans + "ALICE:: " + alice_info_start.toString() + "\n";
		} else {
			ans = ans + "ALICE:: No DATA \n";
		}
		ans = ans + "\n";
		ans = ans + "EOR=" + AliDip2BK.myDateFormat.format(EOR_time) + "\n";

		if (LHC_info_stop != null) {
			ans = ans + "LHC:: " + LHC_info_stop.toString() + "\n";
		} else {
			ans = ans + "LHC:: No DATA \n";
		}
		if (alice_info_stop != null) {
			ans = ans + "ALICE:: " + alice_info_stop.toString() + "\n";
		} else {
			ans = ans + "ALICE:: No DATA \n";
		}

		if (energyHist.size() > 1) {
			ans = ans + " History:: Energy\n";

			for (int i = 0; i < energyHist.size(); i++) {
				TimestampedFloat a1 = energyHist.get(i);
				ans = ans + " - " + AliDip2BK.myDateFormat.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		if (l3_magnetHist.size() > 1) {
			ans = ans + " History:: L3 Magnet\n";

			for (int i = 0; i < l3_magnetHist.size(); i++) {
				TimestampedFloat a1 = l3_magnetHist.get(i);
				ans = ans + " - " + AliDip2BK.myDateFormat.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		if (DipoleMagnetHist.size() > 1) {
			ans = ans + " History:: Dipole Magnet\n";

			for (int i = 0; i < DipoleMagnetHist.size(); i++) {
				TimestampedFloat a1 = DipoleMagnetHist.get(i);
				ans = ans + " - " + AliDip2BK.myDateFormat.format(a1.time) + "  " + a1.value + "\n";
			}
		}

		return ans;

	}

	public void setEORtime(long time) {
		EOR_time = time;
	}

	public void addEnergy(long time, float v) {

		if (energyHist.size() == 0) {
			TimestampedFloat t1 = new TimestampedFloat(time, v);
			energyHist.add(t1);
			return;
		}
		TimestampedFloat v1 = energyHist.get(energyHist.size() - 1);

		double dif = Math.abs(v - v1.value) / v;

		if (dif < AliDip2BK.DIFF_ENERGY) {
			return;
		}
		TimestampedFloat t1 = new TimestampedFloat(time, v);
		energyHist.add(t1);

	}

	public void addL3_magnet(long time, float v) {
		if (l3_magnetHist.size() == 0) {
			TimestampedFloat t2 = new TimestampedFloat(time, v);
			l3_magnetHist.add(t2);
			return;
		}
		TimestampedFloat v2 = l3_magnetHist.get(l3_magnetHist.size() - 1);
		double dif = Math.abs(v - v2.value);
		if (dif < AliDip2BK.DIFF_CURRENT) {
			return;
		} else {
			TimestampedFloat v4 = new TimestampedFloat(time, v);
			l3_magnetHist.add(v4);
		}

	}

	public void addDipoleMagnet(long time, float v) {
		if (DipoleMagnetHist.size() == 0) {
			TimestampedFloat t2 = new TimestampedFloat(time, v);
			DipoleMagnetHist.add(t2);
			return;
		}

		TimestampedFloat v2 = DipoleMagnetHist.get(DipoleMagnetHist.size() - 1);
		double dif = Math.abs(v - v2.value);

		if (dif < AliDip2BK.DIFF_CURRENT) {
			return;
		} else {
			TimestampedFloat v4 = new TimestampedFloat(time, v);
			DipoleMagnetHist.add(v4);
		}

	}

	public float getBeamEnergy() {
		if (LHC_info_start != null) {
			return LHC_info_start.getEnergy();
		} else {
			return -1;
		}
	}

	public String getBeamType() {
		if (LHC_info_start != null) {
			return LHC_info_start.beamType;
		} else {
			return null;
		}
	}

	public String getBeamMode() {
		if (LHC_info_start != null) {
			return LHC_info_start.getBeamMode();
		} else {
			return null;
		}
	}

	public int getFillNo() {
		if (LHC_info_start != null) {
			return LHC_info_start.fillNo;
		} else {
			return -1;
		}
	}

	public int getLHCTotalIneractingBunches() {
		if (LHC_info_start != null) {
			return LHC_info_start.LHCTotalInteractingBunches;
		} else {
			return -1;
		}
	}

	public int getLHCTotalNonInteractingBunchesBeam1() {
		if (LHC_info_start != null) {
			return LHC_info_start.LHCTotalNonInteractingBuchesBeam1;
		} else {
			return -1;
		}
	}

	public int getLHCTotalNonInteractingBunchesBeam2() {
		if (LHC_info_start != null) {
			return LHC_info_start.LHCTotalNonInteractingBuchesBeam2;
		} else {
			return -1;
		}
	}

	public float getLHCBetaStar() {
		if (LHC_info_start != null) {
			return LHC_info_start.getLHCBetaStar();
		} else {
			return -1;
		}
	}

	public String getLHCFillingSchemeName() {
		if (LHC_info_start != null) {
			return LHC_info_start.LHCFillingSchemeName;
		} else {
			return null;
		}
	}

	public String getL3_magnetPolarity() {
		return alice_info_start.L3_polarity;
	}

	public String getDipole_magnetPolarity() {
		return alice_info_start.Dipole_polarity;
	}

	public float getL3_magnetCurrent() {
		return alice_info_start.L3_magnetCurrent;
	}

	public float getDipole_magnetCurrent() {
		return alice_info_start.Dipole_magnetCurrent;
	}

	public void setLuminosityAtStop(LuminosityView luminosityAtStop) {
		this.luminosityAtStop = Optional.of(luminosityAtStop);
	}

	public Optional<Float> getTriggerEfficiency() {
		return luminosityAtStart.triggerEfficiency();
	}

	public Optional<Float> getTriggerAcceptance() {
		return luminosityAtStart.triggerAcceptance();
	}

	public Optional<Float> getCrossSection() {
		return luminosityAtStart.crossSection();
	}

	public Optional<PhaseShift> getPhaseShiftAtStart() {
		return this.luminosityAtStart.phaseShift();
	}

	public Optional<PhaseShift> getPhaseShiftAtStop() {
		return this.luminosityAtStop.flatMap(LuminosityView::phaseShift);
	}
}

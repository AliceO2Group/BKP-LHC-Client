/*************
 * cil
 **************/
/*
 *  Keeps the RUN Information
 */
package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

public class RunInfoObj {

	public int RunNo;
	public LhcInfoObj LHC_info_start;
	public LhcInfoObj LHC_info_stop;
	public final AliceMagnetsConfigurationView magnetsConfigurationAtStart;
	public AliceMagnetsConfigurationView alice_info_stop;
	public List<TimestampedFloat> energyHistory;
	public List<TimestampedFloat> l3CurrentHistory;
	public List<TimestampedFloat> dipoleHistory;
	public long SOR_time;
	public long EOR_time;

	public RunInfoObj(
		long sor_time,
		int RunNo,
		LhcInfoObj fillAtStart,
		AliceMagnetsConfigurationView magnetsConfigurationAtStart
	) {
		this.RunNo = RunNo;
		SOR_time = sor_time;
		this.LHC_info_start = fillAtStart;
		this.magnetsConfigurationAtStart = magnetsConfigurationAtStart;
		energyHistory = new ArrayList<>();
		l3CurrentHistory = new ArrayList<>();
		dipoleHistory = new ArrayList<>();
	}

	public String toString() {
		String ans = "RUN=" + RunNo + "\n";
		ans = ans + "SOR=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(SOR_time) + "\n";
		if (LHC_info_start != null) {
			ans = ans + "LHC:: " + LHC_info_start.toString() + "\n";
		} else {
			ans = ans + "LHC:: No DATA \n";
		}
		if (magnetsConfigurationAtStart != null) {
			ans = ans + "ALICE:: " + magnetsConfigurationAtStart.toString() + "\n";
		} else {
			ans = ans + "ALICE:: No DATA \n";
		}
		ans = ans + "\n";
		ans = ans + "EOR=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(EOR_time) + "\n";

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

		if (!energyHistory.isEmpty()) {
			ans = ans + " History:: Energy\n";

			for (var energy: energyHistory) {
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(energy.time()) + "  " + energy.value() + "\n";
			}
		}

		if (!l3CurrentHistory.isEmpty()) {
			ans = ans + " History:: L3 Magnet\n";

			for (var current: l3CurrentHistory) {
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(current.time()) + "  " + current.value() + "\n";
			}
		}

		if (!dipoleHistory.isEmpty()) {
			ans = ans + " History:: Dipole Magnet\n";

			for (var current: dipoleHistory) {
				ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(current.time()) + "  " + current.value() + "\n";
			}
		}

		return ans;
	}

	public void setEORtime(long time) {
		EOR_time = time;
	}

	public void addEnergy(long time, float energy) {
		this.energyHistory.add(new TimestampedFloat(time, energy));
	}

	public void addL3Current(long time, float current) {
		this.l3CurrentHistory.add(new TimestampedFloat(time, current));
	}

	public void addDipoleMagnet(long time, float current) {
		this.dipoleHistory.add(new TimestampedFloat(time, current));
	}

	public float getBeamEnergy() {
		if (LHC_info_start != null) {
			return LHC_info_start.getEnergy();
		} else {
			return -1;
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

	public float getLHCBetaStar() {
		if (LHC_info_start != null) {
			return LHC_info_start.getLHCBetaStar();
		} else {
			return -1;
		}
	}

	public Optional<Polarity> getL3Polarity() {
		return magnetsConfigurationAtStart.l3Polarity();
	}

	public Optional<Polarity> getDipolePolarity() {
		return magnetsConfigurationAtStart.dipolePolarity();
	}

	public OptionalDouble getL3Current() {
		return magnetsConfigurationAtStart.l3Current();
	}

	public OptionalDouble getDipoleCurrent() {
		return magnetsConfigurationAtStart.dipoleCurrent();
	}
}
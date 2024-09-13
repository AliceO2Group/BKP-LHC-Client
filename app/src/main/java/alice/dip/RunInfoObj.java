/*************
 * cil
 **************/
/*
 *  Keeps the RUN Information
 */
package alice.dip;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

public class RunInfoObj {
	public int RunNo;
	public LhcFillView LHC_info_start;
	public LhcFillView LHC_info_stop;
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
		LhcFillView fillAtStart,
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
		StringBuilder ans = new StringBuilder("RUN=" + RunNo + "\n");
		ans.append("SOR=").append(AliDip2BK.PERSISTENCE_DATE_FORMAT.format(SOR_time)).append("\n");

		ans.append("LHC:: ")
			.append(LHC_info_start != null ? LHC_info_start : "No DATA").append("\n")
			.append("ALICE:: ")
			.append(magnetsConfigurationAtStart != null ? magnetsConfigurationAtStart : "No DATA").append("\n")
			.append("\n")
		;

		ans.append("EOR=").append(AliDip2BK.PERSISTENCE_DATE_FORMAT.format(EOR_time)).append("\n");

		ans.append("LHC:: ")
			.append(LHC_info_stop != null ? LHC_info_stop : "No DATA").append("\n")
			.append("ALICE:: ")
			.append(alice_info_stop != null ? alice_info_stop : "No DATA").append("\n")
		;

		if (!energyHistory.isEmpty()) {
			ans.append(" History:: Energy\n");

			for (var energy: energyHistory) {
				ans.append(" - ").append(AliDip2BK.PERSISTENCE_DATE_FORMAT.format(energy.time())).append("  ").append(energy.value()).append("\n");
			}
		}

		if (!l3CurrentHistory.isEmpty()) {
			ans.append(" History:: L3 Magnet\n");

			for (var current: l3CurrentHistory) {
				ans.append(" - ").append(AliDip2BK.PERSISTENCE_DATE_FORMAT.format(current.time())).append("  ").append(current.value()).append("\n");
			}
		}

		if (!dipoleHistory.isEmpty()) {
			ans.append(" History:: Dipole Magnet\n");

			for (var current: dipoleHistory) {
				ans.append(" - ").append(AliDip2BK.PERSISTENCE_DATE_FORMAT.format(current.time())).append("  ").append(current.value()).append("\n");
			}
		}

		return ans.toString();
	}

	public void setEORTime(long time) {
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
			return LHC_info_start.beamEnergy();
		} else {
			return -1;
		}
	}

	public String getBeamMode() {
		if (LHC_info_start != null) {
			return LHC_info_start.beamMode();
		} else {
			return null;
		}
	}

	public int getFillNo() {
		if (LHC_info_start != null) {
			return LHC_info_start.fillNumber();
		} else {
			return -1;
		}
	}

	public float getLHCBetaStar() {
		return LHC_info_start != null ? LHC_info_start.betaStar() :  -1;
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

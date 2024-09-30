/*************
 * cil
 **************/
/*
 *  Keeps the RUN Information
 */
package alice.dip.core;

import alice.dip.application.AliDip2BK;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

public class RunInfoObj {
	public int RunNo;
	public LhcFillView LHC_info_start;
	public LhcFillView LHC_info_stop;
	public final AliceMagnetsConfigurationView magnetsConfigurationAtStart;
	public AliceMagnetsConfigurationView magnetsConfigurationAtStop;
	public List<TimestampedFloat> energyHistory;
	public List<TimestampedFloat> l3CurrentHistory;
	public List<TimestampedFloat> dipoleHistory;
	public long SOR_time;
	public long EOR_time;
	private final LuminosityView luminosityAtStart;
	private Optional<LuminosityView> luminosityAtStop = Optional.empty();

	public RunInfoObj(
		long startOfRunTime,
		int runNumber,
		LhcFillView fillAtStart,
		LuminosityView luminosityAtStart,
		AliceMagnetsConfigurationView magnetsConfigurationAtStart
	) {
		this.RunNo = runNumber;
		SOR_time = startOfRunTime;
		this.LHC_info_start = fillAtStart;
		this.magnetsConfigurationAtStart = magnetsConfigurationAtStart;
		energyHistory = new ArrayList<>();
		l3CurrentHistory = new ArrayList<>();
		dipoleHistory = new ArrayList<>();
		this.luminosityAtStart = luminosityAtStart;
	}

	public String toString() {
		final StringBuilder ans = new StringBuilder("RUN=" + RunNo + "\n");
		ans.append("SOR=").append(new SerializableDate(SOR_time)).append("\n");

		ans.append("LHC:: ")
			.append(LHC_info_start != null ? LHC_info_start : "No DATA").append("\n")
			.append("ALICE:: ")
			.append(magnetsConfigurationAtStart != null ? magnetsConfigurationAtStart : "No DATA").append("\n")
			.append("\n")
		;

		ans.append("EOR=").append(new SerializableDate(EOR_time)).append("\n");

		ans.append("LHC:: ")
			.append(LHC_info_stop != null ? LHC_info_stop : "No DATA").append("\n")
			.append("ALICE:: ")
			.append(magnetsConfigurationAtStop != null ? magnetsConfigurationAtStop : "No DATA").append("\n")
		;

		luminosityAtStart.crossSection()
            .ifPresent(crossSection -> ans.append(" Cross-section:: ").append(crossSection));
        luminosityAtStart.triggerEfficiency()
            .ifPresent(efficiency -> ans.append(" Trigger efficiency:: ").append(efficiency));
        luminosityAtStart.triggerAcceptance()
            .ifPresent(acceptance -> ans.append(" Trigger acceptance:: ").append(acceptance));
		luminosityAtStart.phaseShift()
			.ifPresent(phaseShift -> ans.append(" Phase shift at start:: ").append(phaseShift));

		getPhaseShiftAtStop().ifPresent(phaseShift -> ans.append(" Phase shift at end:: ").append(phaseShift));

		if (!energyHistory.isEmpty()) {
			ans.append(" History:: Energy\n");

			for (var energy: energyHistory) {
				ans.append(" - ").append(new SerializableDate(energy.time())).append("  ").append(energy.value()).append("\n");
			}
		}

		if (!l3CurrentHistory.isEmpty()) {
			ans.append(" History:: L3 Magnet\n");

			for (var current: l3CurrentHistory) {
				ans.append(" - ").append(new SerializableDate(current.time())).append("  ").append(current.value()).append("\n");
			}
		}

		if (!dipoleHistory.isEmpty()) {
			ans.append(" History:: Dipole Magnet\n");

			for (var current: dipoleHistory) {
				ans.append(" - ").append(new SerializableDate(current.time())).append("  ").append(current.value()).append("\n");
			}
		}

		return ans.toString();
	}

	public void setLuminosityAtStop(LuminosityView luminosityAtStop) {
		this.luminosityAtStop = Optional.of(luminosityAtStop);
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

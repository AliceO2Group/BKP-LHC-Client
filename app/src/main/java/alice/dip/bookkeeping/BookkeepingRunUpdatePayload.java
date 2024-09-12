package alice.dip.bookkeeping;

import alice.dip.PhaseShift;
import alice.dip.Polarity;
import alice.dip.RunInfoObj;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

public class BookkeepingRunUpdatePayload {
	private int runNumber;

	private boolean isEmpty;

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private OptionalDouble lhcBeamEnergy = OptionalDouble.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<String> lhcBeamMode = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private OptionalDouble lhcBetaStar = OptionalDouble.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonProperty("aliceL3Current")
	private OptionalDouble l3MagnetCurrent = OptionalDouble.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonProperty("aliceL3Polarity")
	private Optional<Polarity> l3MagnetPolarity = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonProperty("aliceDipoleCurrent")
	private OptionalDouble dipoleMagnetCurrent = OptionalDouble.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	@JsonProperty("aliceDipolePolarity")
	private Optional<Polarity> dipoleMagnetPolarity = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private OptionalInt fillNumber = OptionalInt.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<Float> crossSection = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<Float> triggerEfficiency = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<Float> triggerAcceptance = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<PhaseShift> phaseShiftAtStart = Optional.empty();

	@JsonInclude(JsonInclude.Include.NON_ABSENT)
	private Optional<PhaseShift> phaseShiftAtEnd = Optional.empty();

	public BookkeepingRunUpdatePayload(int runNumber) {
		this.runNumber = runNumber;
		this.isEmpty = true;
	}

	public static BookkeepingRunUpdatePayload of(RunInfoObj runInfoObj) {
		var updateRunRequest = new BookkeepingRunUpdatePayload(runInfoObj.RunNo);

		float beamEnergy = runInfoObj.getBeamEnergy();
		if (beamEnergy > 0) {
			updateRunRequest.setBeamEnergy(beamEnergy);
		}

		String beamMode = runInfoObj.getBeamMode();
		if (beamMode != null) {
			updateRunRequest.setBeamMode(beamMode);
		}

		var l3MagnetCurrent = runInfoObj.getL3Current();
		if(l3MagnetCurrent.isPresent()) {
			var current = l3MagnetCurrent.getAsDouble();
			updateRunRequest.setL3MagnetCurrent(current);

			var l3MagnetPolarity = runInfoObj.getL3Polarity();
			if (l3MagnetPolarity.isPresent() && current > 0) {
				updateRunRequest.setL3MagnetPolarity(l3MagnetPolarity.get());
			}
		}

		var dipoleMagnetCurrent = runInfoObj.getDipoleCurrent();
		if (dipoleMagnetCurrent.isPresent()) {
			var current = dipoleMagnetCurrent.getAsDouble();
			updateRunRequest.setDipoleMagnetCurrent(current);

			var dipoleMagnetPolarity= runInfoObj.getDipolePolarity();
			if (dipoleMagnetPolarity.isPresent() && current > 0) {
				updateRunRequest.setDipoleMagnetPolarity(dipoleMagnetPolarity.get());
			}
		}

		int fillNumber = runInfoObj.getFillNo();
		if (fillNumber > 0) {
			updateRunRequest.setFillNumber(fillNumber);
		}

		float betaStar = runInfoObj.getLHCBetaStar();
		if (betaStar >= 0) {
			updateRunRequest.setBetaStar(betaStar);
		}

		if (runInfoObj.getTriggerEfficiency().isPresent()) {
			updateRunRequest.setTriggerEfficiency(runInfoObj.getTriggerEfficiency().get());
		}

		if (runInfoObj.getTriggerAcceptance().isPresent()) {
			updateRunRequest.setTriggerAcceptance(runInfoObj.getTriggerAcceptance().get());
		}

		if (runInfoObj.getCrossSection().isPresent()) {
			updateRunRequest.setCrossSection(runInfoObj.getCrossSection().get());
		}

		if (runInfoObj.getPhaseShiftAtStart().isPresent()) {
			updateRunRequest.setPhaseShiftAtStart(runInfoObj.getPhaseShiftAtStart().get());
		}

		if (runInfoObj.getPhaseShiftAtStop().isPresent()) {
			updateRunRequest.setPhaseShiftAtEnd(runInfoObj.getPhaseShiftAtStop().get());
		}

		return updateRunRequest;
	}

	public boolean isEmpty() {
		return this.isEmpty;
	}

	public int getRunNumber() {
		return runNumber;
	}

	private void setBeamEnergy(double beamEnergy) {
		this.lhcBeamEnergy = OptionalDouble.of(beamEnergy);
		this.isEmpty = false;
	}

	private void setBeamMode(String beamMode) {
		this.lhcBeamMode = Optional.of(beamMode);
		this.isEmpty = false;
	}

	private void setL3MagnetCurrent(double current) {
		this.l3MagnetCurrent = OptionalDouble.of(current);
		this.isEmpty = false;
	}

	private void setL3MagnetPolarity(Polarity polarity) {
		this.l3MagnetPolarity = Optional.of(polarity);
		this.isEmpty = false;
	}

	private void setDipoleMagnetCurrent(double current) {
		this.dipoleMagnetCurrent = OptionalDouble.of(current);
		this.isEmpty = false;
	}

	private void setDipoleMagnetPolarity(Polarity polarity) {
		this.dipoleMagnetPolarity = Optional.of(polarity);
		this.isEmpty = false;
	}

	private void setFillNumber(int fillNumber) {
		this.fillNumber = OptionalInt.of(fillNumber);
		this.isEmpty = false;
	}

	private void setBetaStar(double betaStar) {
		this.lhcBetaStar = OptionalDouble.of(betaStar);
		this.isEmpty = false;
	}

	private void setTriggerEfficiency(float triggerEfficiency) {
		this.triggerEfficiency = Optional.of(triggerEfficiency);
		this.isEmpty = false;
	}

	private void setTriggerAcceptance(float triggerAcceptance) {
		this.triggerAcceptance = Optional.of(triggerAcceptance);
		this.isEmpty = false;
	}

	private void setCrossSection(float crossSection) {
		this.crossSection = Optional.of(crossSection);
		this.isEmpty = false;
	}

	private void setPhaseShiftAtStart(PhaseShift phaseShift) {
		this.phaseShiftAtStart = Optional.of(phaseShift);
		this.isEmpty = false;
	}

	public void setPhaseShiftAtEnd(PhaseShift phaseShift) {
		this.phaseShiftAtEnd = Optional.of(phaseShift);
		this.isEmpty = false;
	}

	// Getters for jackson

	public OptionalDouble getLhcBeamEnergy() {
		return lhcBeamEnergy;
	}

	public Optional<String> getLhcBeamMode() {
		return lhcBeamMode;
	}

	public OptionalDouble getLhcBetaStar() {
		return lhcBetaStar;
	}

	public OptionalDouble getL3MagnetCurrent() {
		return l3MagnetCurrent;
	}

	public Optional<Polarity> getL3MagnetPolarity() {
		return l3MagnetPolarity;
	}

	public OptionalDouble getDipoleMagnetCurrent() {
		return dipoleMagnetCurrent;
	}

	public Optional<Polarity> getDipoleMagnetPolarity() {
		return dipoleMagnetPolarity;
	}

	public OptionalInt getFillNumber() {
		return fillNumber;
	}

	public Optional<Float> getTriggerEfficiency() {
		return triggerEfficiency;
	}

	public Optional<Float> getTriggerAcceptance() {
		return triggerAcceptance;
	}

	public Optional<Float> getCrossSection() {
		return crossSection;
	}

	public Optional<PhaseShift> getPhaseShiftAtStart() {
		return phaseShiftAtStart;
	}

	public Optional<PhaseShift> getPhaseShiftAtEnd() {
		return phaseShiftAtEnd;
	}
}

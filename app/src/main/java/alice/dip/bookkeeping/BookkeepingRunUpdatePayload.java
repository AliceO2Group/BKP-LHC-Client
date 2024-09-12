package alice.dip.bookkeeping;

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

	public BookkeepingRunUpdatePayload(int runNumber) {
		this.runNumber = runNumber;
		this.isEmpty = true;
	}

	public static BookkeepingRunUpdatePayload of(RunInfoObj runInfoObj) {
		var updateRunRequest = new BookkeepingRunUpdatePayload(runInfoObj.RunNo);

		float beamEnergy = runInfoObj.getBeamEnergy();
		if (beamEnergy > 0) {
			updateRunRequest.beamEnergy(beamEnergy);
		}

		String beamMode = runInfoObj.getBeamMode();
		if (beamMode != null) {
			updateRunRequest.beamMode(beamMode);
		}

		var l3MagnetCurrent = runInfoObj.getL3Current();
		if(l3MagnetCurrent.isPresent()) {
			var current = l3MagnetCurrent.getAsDouble();
			updateRunRequest.l3MagnetCurrent(current);

			var l3MagnetPolarity = runInfoObj.getL3Polarity();
			if (l3MagnetPolarity.isPresent() && current > 0) {
				updateRunRequest.l3MagnetPolarity(l3MagnetPolarity.get());
			}
		}

		var dipoleMagnetCurrent = runInfoObj.getDipoleCurrent();
		if (dipoleMagnetCurrent.isPresent()) {
			var current = dipoleMagnetCurrent.getAsDouble();
			updateRunRequest.dipoleMagnetCurrent(current);

			var dipoleMagnetPolarity = runInfoObj.getDipolePolarity();
			if (dipoleMagnetPolarity.isPresent() && current > 0) {
				updateRunRequest.dipoleMagnetPolarity(dipoleMagnetPolarity.get());
			}
		}

		int fillNumber = runInfoObj.getFillNo();
		if (fillNumber > 0) {
			updateRunRequest.fillNumber(fillNumber);
		}

		float betaStar = runInfoObj.getLHCBetaStar();
		if (betaStar >= 0) {
			updateRunRequest.betaStar(betaStar);
		}

		return updateRunRequest;
	}

	public boolean isEmpty() {
		return this.isEmpty;
	}

	public int getRunNumber() {
		return runNumber;
	}

	public void beamEnergy(double lhcBeamEnergy) {
		this.lhcBeamEnergy = OptionalDouble.of(lhcBeamEnergy);
		this.isEmpty = false;
	}

	public void beamMode(String lhcBeamMode) {
		this.lhcBeamMode = Optional.of(lhcBeamMode);
		this.isEmpty = false;
	}

	public void betaStar(double lhcBetaStar) {
		this.lhcBetaStar = OptionalDouble.of(lhcBetaStar);
		this.isEmpty = false;
	}

	public void l3MagnetCurrent(double current) {
		this.l3MagnetCurrent = OptionalDouble.of(current);
		this.isEmpty = false;
	}

	public void l3MagnetPolarity(Polarity polarity) {
		this.l3MagnetPolarity = Optional.of(polarity);
		this.isEmpty = false;
	}

	public void dipoleMagnetCurrent(double current) {
		this.dipoleMagnetCurrent = OptionalDouble.of(current);
		this.isEmpty = false;
	}

	public void dipoleMagnetPolarity(Polarity polarity) {
		this.dipoleMagnetPolarity = Optional.of(polarity);
		this.isEmpty = false;
	}

	public void fillNumber(int fillNumber) {
		this.fillNumber = OptionalInt.of(fillNumber);
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
}

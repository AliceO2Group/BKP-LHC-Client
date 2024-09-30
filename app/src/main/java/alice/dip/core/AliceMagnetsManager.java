package alice.dip.core;

import java.util.Optional;
import java.util.OptionalDouble;

public class AliceMagnetsManager {
	private OptionalDouble l3Current = OptionalDouble.empty();
	private Optional<Polarity> l3Polarity = Optional.empty();

	private OptionalDouble dipoleCurrent = OptionalDouble.empty();
	private Optional<Polarity> dipolePolarity = Optional.empty();

	/**
	 * Return a record of the current magnets configuration
	 *
	 * @return the magnets configuration record
	 */
	public AliceMagnetsConfigurationView getView() {
		return new AliceMagnetsConfigurationView(l3Current, l3Polarity, dipoleCurrent, dipolePolarity);
	}

	public void setL3Current(double current) {
		l3Current = OptionalDouble.of(current);
	}

	public void setL3Polarity(Polarity polarity) {
		l3Polarity = Optional.of(polarity);
	}

	public void setDipoleCurrent(double current) {
		dipoleCurrent = OptionalDouble.of(current);
	}

	public void setDipolePolarity(Polarity polarity) {
		dipolePolarity = Optional.of(polarity);
	}
}

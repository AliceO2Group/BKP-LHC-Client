/*************
 * cil
 **************/
/*
 * Keeps ALICE specific information
 */
package alice.dip.core;

import java.util.Optional;
import java.util.OptionalDouble;

public record AliceMagnetsConfigurationView(OptionalDouble l3Current, Optional<Polarity> l3Polarity, OptionalDouble dipoleCurrent, Optional<Polarity> dipolePolarity) {
	@Override
	public String toString() {
		String ans = "L3_Magnet_Current=";

		if (l3Current.isPresent()) {
			ans = ans + l3Current.getAsDouble() + " Polarity:" + l3Polarity;
		} else {
			ans = ans + "No Data";
		}

		ans = ans + " Dipole_Magnet_Current=";
		if (dipoleCurrent.isPresent()) {
			ans = ans + dipoleCurrent.getAsDouble() + " Polarity:" + dipolePolarity;
		} else {
			ans = ans + "No Data";
		}
		return ans;
	}
}
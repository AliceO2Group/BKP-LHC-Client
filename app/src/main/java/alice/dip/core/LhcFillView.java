package alice.dip.core;

import java.util.Optional;

public record LhcFillView(
	int fillNumber,
	SerializableDate startTime,
	Optional<SerializableDate> endTime,
	Optional<SerializableDate> stableBeamStart,
	Optional<SerializableDate> stableBeamStop,
	int stableBeamsDuration,
	String beamMode,
	String beamType,
	String fillingSchemeName,
	float beamEnergy,
	float betaStar,
	int totalInteractingBunches,
	int interactingBunchesBeam1
) {
	@Override
	public String toString() {
		String ans = " FILL No=" + fillNumber + " StartTime=" + startTime;
		if (endTime.isPresent()) {
			ans += " EndTime=" + endTime.get();
		}
		ans += " Beam Mode=" + beamMode;
		ans += " Beam Type=" + beamType;
		ans += " LHC Filling Scheme =" + fillingSchemeName;
		ans += " Beam  Energy=" + beamEnergy + " Beta Star=" + betaStar;
		ans += " LHCTotalInteractingBunches =" + totalInteractingBunches
			+ " LHCTotalNonInteractingBunchesBeam1=" + interactingBunchesBeam1;
		if (stableBeamStart.isPresent()) {
			ans += " Stable Beam start=" + stableBeamStart.get();

			if (stableBeamStop.isPresent()) {
				ans += " Stable Beam stop=" + stableBeamStop.get();
			}
		}
		ans += " Stable Beam Duration=" + stableBeamsDuration;
		return ans;
	}
}

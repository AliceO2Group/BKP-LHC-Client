package alice.dip;

import java.util.Optional;

public record LhcFillView(
    int fillNumber,
    long startTime,
    long endTime,
    Optional<Long> stableBeamStart,
    Optional<Long> stableBeamStop,
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
        String ans = " FILL No=" + fillNumber + " StartTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(startTime);
        if (endTime > 0) {
            ans += " EndTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(endTime);
        }
        ans += " Beam Mode=" + beamMode;
        ans += " Beam Type=" + beamType;
        ans += " LHC Filling Scheme =" + fillingSchemeName;
        ans += " Beam  Energy=" + beamEnergy + " Beta Star=" + betaStar;
        ans += " LHCTotalInteractingBunches =" + totalInteractingBunches
            + " LHCTotalNonInteractingBunchesBeam1=" + interactingBunchesBeam1;
        ans += " Stable Beam Duration=" + stableBeamsDuration;
        return ans;
    }
}

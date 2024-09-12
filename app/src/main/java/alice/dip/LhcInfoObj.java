/*************
 * cil
 **************/
/*
 * Keeps the required LHC information
 */
package alice.dip;

import alice.dip.configuration.PersistenceConfiguration;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

public class LhcInfoObj implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private static final String BEAM_MODE_STABLE_BEAMS = "STABLE BEAMS";

    public final int fillNo;
    public String lhcFillingSchemeName;
    public int ip2CollisionsCount;
    public int bunchesCount;

    private final long createdTime;
    private final String beam1ParticleType;
    private final String beam2ParticleType;
    private final String beamType;
    private final int lhcTotalInteractingBunches;
    private final int lhcTotalNonInteractingBunchesBeam1;
    private final HistorizedFloat beamEnergyHistory;
    private final HistorizedFloat betaStarHistory;
    private final HistorizedString beamModeHistory;
    private final List<TimestampedString> fillingSchemeHistory;

    private long endedTime = -1;

    public LhcInfoObj(
        PersistenceConfiguration persistenceConfiguration,
        long date,
        int fillNumber,
        String beam1ParticleType,
        String beam2ParticleType,
        String fillingScheme,
        int ip2CollisionsCount,
        int bunchesCount
    ) {
        createdTime = date;
        fillNo = fillNumber;

        this.beam1ParticleType = beam1ParticleType;
        this.beam2ParticleType = beam2ParticleType;
        beamType = beam1ParticleType + " - " + beam2ParticleType;
        lhcFillingSchemeName = fillingScheme;
        this.ip2CollisionsCount = ip2CollisionsCount;
        this.bunchesCount = bunchesCount;

        beamModeHistory = new HistorizedString();
        fillingSchemeHistory = List.of(new TimestampedString(date, fillingScheme + " *"));
        beamEnergyHistory = new HistorizedFloat(persistenceConfiguration.minimumLoggedEnergyDelta());
        betaStarHistory = new HistorizedFloat(persistenceConfiguration.minimumLoggedBetaDelta());

        lhcTotalInteractingBunches = this.ip2CollisionsCount;
        lhcTotalNonInteractingBunchesBeam1 = Math.max(0, this.bunchesCount - this.ip2CollisionsCount);
    }

    public LhcFillView getView() {
        return new LhcFillView(
            fillNo,
            createdTime,
            endedTime,
            getStableBeamStart(),
            getStableBeamStop(),
            getStableBeamDuration(),
            getBeamMode(),
            beamType,
            lhcFillingSchemeName,
            beamEnergyHistory.current().map(TimestampedFloat::value).orElse(-1.f),
            betaStarHistory.current().map(TimestampedFloat::value).orElse(-1.f),
            lhcTotalInteractingBunches,
            lhcTotalNonInteractingBunchesBeam1
        );
    }

    public String history() {
        String ans = " FILL No=" + fillNo + " StartTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(createdTime);
        if (endedTime > 0) {
            ans = ans + " EndTime=" + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(endedTime) + "\n";
        }
        ans = ans + " Beam1_ParticleType=" + beam1ParticleType + " Beam2_ParticleType=" + beam2ParticleType;
        ans = ans + " Beam Type=" + beamType;
        ans = ans + " LHC Filling Scheme =" + lhcFillingSchemeName + "\n";

        var beamEnergy = beamEnergyHistory
            .current().map(timestampedFloat -> String.valueOf(timestampedFloat.value()))
            .orElse("-");
        var betaStar = betaStarHistory.current()
            .map(timestampedFloat -> String.valueOf(timestampedFloat.value()))
            .orElse("-");

        ans = ans + " Beam Energy=" + beamEnergy + " Beta Star=" + betaStar + "\n";
        ans = ans + " No_BUNCHES=" + bunchesCount + "\n";
        ans = ans + " LHCTotalInteractingBunches =" + lhcTotalInteractingBunches + " LHCTotalNonInteractingBuchesBeam1="
            + lhcTotalNonInteractingBunchesBeam1 + "\n";
        ans = ans + " Start Stable Beams =" + getStableBeamStartStr();
        ans = ans + " Stop Stable Beams =" + getStableBeamStopStr() + "\n";
        ans = ans + " Stable Beam Duration [s] =" + getStableBeamDuration() + "\n";

        if (!beamModeHistory.isEmpty()) {
            ans = ans + " History:: Beam Mode\n";

            for (TimestampedString a1 : beamModeHistory) {
                ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
            }
        }

        if (!fillingSchemeHistory.isEmpty()) {
            ans = ans + " History:: Filling Scheme \n";

            for (TimestampedString a1 : fillingSchemeHistory) {
                ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
            }
        }

        if (!beamEnergyHistory.isEmpty()) {
            ans = ans + " History:: Beam Energy\n";
            for (TimestampedFloat a1 : beamEnergyHistory) {
                ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
            }
        }

        if (!betaStarHistory.isEmpty()) {
            ans = ans + " History:: LHC Beta Star\n";

            for (TimestampedFloat a1 : betaStarHistory) {
                ans = ans + " - " + AliDip2BK.PERSISTENCE_DATE_FORMAT.format(a1.time()) + "  " + a1.value() + "\n";
            }
        }

        return ans;
    }

    public void saveFillingSchemeInHistory(long time, String fillingScheme, boolean isBeamPhysicsInjection) {
        TimestampedString ts2 = new TimestampedString(
            time,
            isBeamPhysicsInjection ? fillingScheme + " *" : fillingScheme
        );
        fillingSchemeHistory.add(ts2);
    }

    public void setBeamMode(long date, String mode) {
        beamModeHistory.push(date, mode);
    }

    public void setEnergy(long date, float v) {
        beamEnergyHistory.push(date, v);
    }

    public void setLHCBetaStar(long date, float v) {
        betaStarHistory.push(date, v);
    }

    public String getBeamMode() {
        return beamModeHistory.current().map(TimestampedString::value).orElse(null);
    }

    public Optional<Long> getStableBeamStart() {
        for (TimestampedString a1 : beamModeHistory) {
            if (a1.value().equalsIgnoreCase(BEAM_MODE_STABLE_BEAMS)) {
                return Optional.of(a1.time());
            }
        }

        return Optional.empty();
    }

    public Optional<Long> getStableBeamStop() {
        var sbs = getStableBeamStart();
        if (sbs.isEmpty()) {
            return Optional.empty();
        }

        // If beam mode history is empty or do not contain stable beam mode, return empty (case 1)
        // If stable beam is the last element of beam mode history:
        // - if fill is still active, return 0 (case 2)
        // - if we have an end time, return it (case 3)
        // Else, return the timestamp of the item following stable beam (which mark the end of SB) (case 4)

        var iterator = beamModeHistory.iterator();
        while (iterator.hasNext()) {
            var beamMode = iterator.next();
            if (beamMode.value().equalsIgnoreCase(BEAM_MODE_STABLE_BEAMS)) {
                if (iterator.hasNext()) {
                    // There is a beam mode after stable beams (case 4)
                    return Optional.of(iterator.next().time());
                } else {
                    // Currently in stable beams (case 2 or case 3)
                    return Optional.of(endedTime >= 0 ? endedTime : 0);
                }
            }
        }

        // Did not find stable beam mode (case 1)
        return Optional.empty();
    }

    /**
     * Return the stable beams duration (in ms)
     *
     * @return stable beams duration
     */
    public int getStableBeamDuration() {
        long duration = 0;

        var iterator = beamModeHistory.iterator();

        var beamMode = iterator.hasNext() ? iterator.next() : null;
        var nextBeamMode = iterator.hasNext() ? iterator.next() : null;

        // Loop over all the beam mode, and sums the duration of the stable beams ones
        while (beamMode != null) {
            if (beamMode.value().equalsIgnoreCase(BEAM_MODE_STABLE_BEAMS)) {
                if (nextBeamMode != null) {
                    // It is not the current beam
                    duration += nextBeamMode.time() - beamMode.time();
                } else {
                    // It is the current beam mode
                    var end = endedTime >= 0 ? endedTime : new Date().getTime();
                    duration += end - beamMode.time();
                }
            }

            beamMode = nextBeamMode;
            nextBeamMode = iterator.hasNext() ? iterator.next() : null;
        }

        return (int) (duration / 1000);
    }

    public void setEnd(long date) {
        endedTime = date;
    }

    private String getStableBeamStartStr() {
        var stableBeamStart = getStableBeamStart();

        return stableBeamStart.map(AliDip2BK.PERSISTENCE_DATE_FORMAT::format).orElse("No Stable Beam");
    }

    private String getStableBeamStopStr() {
        if (getStableBeamStart().isEmpty()) {
            return "No Stable Beam";
        }

        return getStableBeamStop().map(AliDip2BK.PERSISTENCE_DATE_FORMAT::format).orElse("-");
    }
}

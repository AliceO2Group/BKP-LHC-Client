package alice.dip;

import java.util.Optional;

public record LuminosityView(
    Optional<Float> triggerAcceptance,
    Optional<Float> triggerEfficiency,
    Optional<Float> crossSection,
    Optional<PhaseShift> phaseShift
) {
    @Override
    public String toString() {
        var ans = " TriggerAcceptance=" + triggerAcceptance;
        ans += " CrossSection=" + crossSection;
        ans += " Efficiency=" + triggerEfficiency;
        if (phaseShift.isPresent()) {
            ans += " PhaseShiftBeam1=" + phaseShift.get().beam1();
            ans += " PhaseShiftBeam2=" + phaseShift.get().beam2();
        }

        return ans;
    }
}

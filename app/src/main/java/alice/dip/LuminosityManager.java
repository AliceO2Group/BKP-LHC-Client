package alice.dip;

import java.util.Optional;

public class LuminosityManager {
    private Optional<Float> triggerAcceptance;
    private Optional<Float> triggerEfficiency;
    private Optional<Float> crossSection;
    private Optional<PhaseShift> phaseShift;

    public LuminosityView getView() {
        return new LuminosityView(triggerAcceptance, triggerEfficiency, crossSection, phaseShift);
    }

    public void setTriggerAcceptance(float triggerAcceptance) {
        this.triggerAcceptance = Optional.of(triggerAcceptance);
    }

    public void setTriggerEfficiency(float triggerEfficiency) {
        this.triggerEfficiency = Optional.of(triggerEfficiency);
    }

    public void setCrossSection(float crossSection) {
        this.crossSection = Optional.of(crossSection);
    }

    public void setPhaseShift(PhaseShift phaseShift) {
        this.phaseShift = Optional.of(phaseShift);
    }
}

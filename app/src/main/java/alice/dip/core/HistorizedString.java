package alice.dip.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class HistorizedString implements Iterable<TimestampedString> {
    private final List<TimestampedString> history = new ArrayList<>();

    /**
     * States if the history is empty
     *
     * @return false if the history has at least one value, else return true
     */
    boolean isEmpty() {
        return history.isEmpty();
    }

    Optional<TimestampedString> current() {
        return history.isEmpty()
            ? Optional.empty()
            : Optional.of(history.get(history.size() - 1));
    }

    /**
     * Save a new value in the history
     *
     * @param time the time of the value
     * @param value the actual value
     */
    void push(long time, String value) {
        history.add(new TimestampedString(time, value));
    }

    @Override
    public Iterator<TimestampedString> iterator() {
        return history.iterator();
    }
}

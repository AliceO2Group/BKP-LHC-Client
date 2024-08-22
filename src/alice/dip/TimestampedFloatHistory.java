package alice.dip;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimestampedFloatHistory implements Iterable<TimestampedFloat> {
	private final List<TimestampedFloat> history = new ArrayList<>();
	private final float delta;

	/**
	 * Constructor
	 *
	 * @param delta minimum difference between a value and the previous one for it to be stored
	 */
	TimestampedFloatHistory(float delta) {
		this.delta = delta;
	}

	/**
	 * States if the history is empty
	 *
	 * @return false if the history has at least one value, else return true
	 */
	boolean isEmpty() {
		return history.isEmpty();
	}

	/**
	 * Save a new value in the history
	 *
	 * @param time  the time of the value
	 * @param value the actual value
	 */
	void push(long time, float value) {
		if (history.isEmpty()) {
			TimestampedFloat newValue = new TimestampedFloat(time, value);
			history.add(newValue);
		} else {
			TimestampedFloat lastValue = history.get(history.size() - 1);

			float diff = Math.abs(value - lastValue.value()) / value;

			if (diff < delta) {
				return;
			}

			TimestampedFloat newValue = new TimestampedFloat(time, value);
			history.add(newValue);
		}
	}

	@Override
	public Iterator<TimestampedFloat> iterator() {
		return history.iterator();
	}
}

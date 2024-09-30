package alice.dip.core;

import java.text.SimpleDateFormat;

/**
 * Serializable date
 *
 * @param time UNIX UTC timestamp of the date to represent, in milliseconds
 */
public class SerializableDate {
	private static final String DEFAULT_PERSISTENCE_DATE_FORMAT = "dd-MM-yy HH:mm";

	// For now dateFormat is final, but it might be editable if there is need for it
	private final SimpleDateFormat dateFormat = new SimpleDateFormat(DEFAULT_PERSISTENCE_DATE_FORMAT);

	private final long time;

	public SerializableDate(long time) {
		this.time = time;
	}

	@Override
	public String toString() {
		return dateFormat.format(time);
	}
}

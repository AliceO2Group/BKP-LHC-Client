package alice.dip;

public enum Polarity {
	POSITIVE("Positive"),
	NEGATIVE("Negative");

	private final String display;
	Polarity(String display) {
		this.display = display;
	}

	@Override
	public String toString() {
		return display;
	}
}

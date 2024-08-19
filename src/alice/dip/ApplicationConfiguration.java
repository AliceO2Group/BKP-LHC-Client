package alice.dip;

public record ApplicationConfiguration(
	BookkeepingClientConfiguration bookkeepingClientConfiguration
) {
	public record BookkeepingClientConfiguration(String url, String token) {
	}
}

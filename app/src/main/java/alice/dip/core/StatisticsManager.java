package alice.dip.core;

public class StatisticsManager {
	private int dipMessagesCount = 0;
	private int kafkaMessagesCount = 0;
	private int newFillsCount = 0;
	private int newRunsCount = 0;
	private int endedRunsCount = 0;
	private int duplicatedRunsEndCount = 0;

	public int getDipMessagesCount() {
		return dipMessagesCount;
	}
	public void incrementDipMessagesCount() {
		dipMessagesCount++;
	}

	public int getKafkaMessagesCount() {
		return kafkaMessagesCount;
	}

	public void incrementKafkaMessagesCount() {
		kafkaMessagesCount++;
	}

	public int getNewFillsCount() {
		return newFillsCount;
	}

	public void incrementNewFillsCount() {
		newFillsCount++;
	}

	public int getNewRunsCount() {
		return newRunsCount;
	}

	public void incrementNewRunsCount() {
		newRunsCount++;
	}

	public int getEndedRunsCount() {
		return endedRunsCount;
	}

	public void incrementEndedRunsCount() {
		endedRunsCount++;
	}

	public int getDuplicatedRunsEndCount() {
		return duplicatedRunsEndCount;
	}

	public void incrementDuplicatedRunsEndCount() {
		duplicatedRunsEndCount++;
	}
}

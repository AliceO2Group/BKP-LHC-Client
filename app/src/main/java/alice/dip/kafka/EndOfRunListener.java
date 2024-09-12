package alice.dip.kafka;

public interface EndOfRunListener {
	void onEndOfRun(long date, int runNumber);
}

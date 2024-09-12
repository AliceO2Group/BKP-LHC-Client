package alice.dip.kafka;

public interface StartOfRunListener {
	void onNewRun(long date, int runNumber);
}

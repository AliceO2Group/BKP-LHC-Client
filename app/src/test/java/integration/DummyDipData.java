package integration;

import cern.dip.DipTimestamp;
import cern.dip.implementation.DipDataImp;

import java.util.Date;

public class DummyDipData extends DipDataImp {
	public DummyDipData() {
		this.setDipTime(new DipTimestamp(new Date().getTime()));
	}
}

package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;

import org.joda.time.DateTime;

public class KMeansDateTimeDetectionTest extends TestCase {
	
	private static final int NUM_SAMPLES = 20;
	
	@Test
	public void testDetection() throws Exception {
		KMeansDateTimeDetection detector = new KMeansDateTimeDetection();
		
		List<DateTime> testData = new ArrayList<DateTime>();
		
		for (int i = 0; i < NUM_SAMPLES; ++i) {
			int year = 2016;
			int month = 2;
			int day = (i / 7) * 7 + i % 5 + 1;
			int hour = 0;
			int minute = 0;
			DateTime currentDate = new DateTime(year, month, day, hour, minute);
			for (int j = 0; j < 20; ++j) {
				testData.add(currentDate);
			}
		}
		
		DateTime okDate = new DateTime("2016-02-01T00:00Z");
		DateTime koDate = new DateTime("2016-02-07T00:00Z");
		
		assertTrue(detector.detect(testData, okDate));
		assertFalse(detector.detect(testData, koDate));
		
		detector.close();
	}
}

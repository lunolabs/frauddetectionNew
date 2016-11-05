package ru.spbstu.frauddetection.detection;

import java.util.ArrayList;

import java.util.Date;
import org.joda.time.DateTime;

import org.junit.Test;
import junit.framework.TestCase;

public class QuantilleDateTimeDetectionTest extends TestCase {
	
	private static final long NUM_TESTS = 1000; 
	
	@Test
	public void testDateTime() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 9 + i % 9; //Hours: 9 - 17
			int date = 1 + i % 7; //Day: 1 - 7 of Month
			int month = 1 + i % 3; //Month: 1 - 3 of Year
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
	
		DateTime dateTimeTrue = new DateTime("2016-02-05T12:00Z"); //5th of February 2016, 12:00
		DateTime dateTimeFalseMonth = new DateTime("2016-11-01T12:00Z"); //1st of November 2016, 12:00
		DateTime dateTimeFalseDay = new DateTime("2016-02-24T12:00Z"); //24th of February 2016, 12:00
		DateTime dateTimeFalseHour = new DateTime("2016-02-06T21:00Z"); //6th of February 2016, 21:00
	
		assertTrue(qddt.detect(testData, dateTimeTrue));
		assertFalse(qddt.detect(testData, dateTimeFalseMonth));
		assertFalse(qddt.detect(testData, dateTimeFalseDay));
		assertFalse(qddt.detect(testData, dateTimeFalseHour));
		
		qddt.close();
	}
	
	@Test
	public void testDate() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<Date> testData = new ArrayList<Date>();
		
		for (int i = 0; i < NUM_TESTS; ++i) {
			Date curDate = new Date();
			curDate.setTime(new DateTime(2016, 1 + i % 3, 1 + i % 7, 9 + i % 9, 0).getMillis());
			testData.add(curDate);
		}
		
		Date dateTrue = new Date();
		dateTrue.setTime(new DateTime("2016-02-05T12:00Z").getMillis());
		Date dateFalseMonth = new Date();
		dateFalseMonth.setTime(new DateTime("2016-11-01T12:00Z").getMillis());
		Date dateFalseDay = new Date();
		dateFalseDay.setTime(new DateTime("2016-02-24T12:00Z").getMillis());
		Date dateFalseHour = new Date();
		dateFalseHour.setTime(new DateTime("2016-02-06T21:00Z").getMillis());
		
		assertTrue(qddt.detect(testData, dateTrue));
		assertFalse(qddt.detect(testData, dateFalseMonth));
		assertFalse(qddt.detect(testData, dateFalseDay));
		assertFalse(qddt.detect(testData, dateFalseHour));
		
		qddt.close();
	}
	
	@Test
	public void testDayOfWeek() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 0;
			int date = 4 + i % 5; //Day: 4 - 8 of January (Monday - Friday)
			int month = 1; //Month: 1 - 3 of Year
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-05T00:00Z");
		DateTime dateTimeFalse = new DateTime("2016-01-10T00:00Z");
		assertTrue(qddt.detectDayOfWeek(testData, dateTimeTrue));
		assertFalse(qddt.detectDayOfWeek(testData, dateTimeFalse));
		
		qddt.close();
	}
	
	@Test
	public void testDayOfMonth() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 0;
			int date = 1 + i % 7; //Day: 01-07 of Month
			int month = 1 + i % 3; //Months 01 - 03 of Year
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-05T00:00Z");
		DateTime dateTimeFalse = new DateTime("2016-01-10T00:00Z");
		assertTrue(qddt.detectDayOfMonth(testData, dateTimeTrue));
		assertFalse(qddt.detectDayOfMonth(testData, dateTimeFalse));
		
		qddt.close();
	}
	
	@Test
	public void testDayOfYear() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 0;
			int date = 1 + i % 7; //Day: 01-07 of Month
			int month = 1;
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-04T00:00Z");
		DateTime dateTimeFalse = new DateTime("2016-10-10T00:00Z");
		assertTrue(qddt.detectDayOfYear(testData, dateTimeTrue));
		assertFalse(qddt.detectDayOfYear(testData, dateTimeFalse));
		
		qddt.close();
	}
	
	@Test
	public void testWeekOfYear() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 0;
			int date = 1 + i % 31; //Day: 01-31 of Month (first 5 weeks of Year)
			int month = 1;
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-11T00:00Z");
		DateTime dateTimeFalse = new DateTime("2016-02-10T00:00Z");
		assertTrue(qddt.detectWeekOfYear(testData, dateTimeTrue));
		assertFalse(qddt.detectWeekOfYear(testData, dateTimeFalse));
		
		qddt.close();
	}
	
	@Test
	public void testMonthOfYear() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = 0;
			int hour = 0;
			int date = 1; //Day: 01 of Month
			int month = 1 + i % 3; //Months 01 - 03 of Year
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-05T00:00Z");
		DateTime dateTimeFalse = new DateTime("2016-07-10T00:00Z");
		assertTrue(qddt.detectMonthOfYear(testData, dateTimeTrue));
		assertFalse(qddt.detectMonthOfYear(testData, dateTimeFalse));
		
		qddt.close();
	}
	
	@Test
	public void testTimeOfDay() throws Exception {
		QuantilleDateTimeDetection qddt = new QuantilleDateTimeDetection();
		ArrayList<DateTime> testData = new ArrayList<DateTime>();

		for (int i = 0; i < NUM_TESTS; ++i) {
			int minute = i % 60;
			int hour = 9 + i % 9; // Hours 09 - 17
			int date = 1;
			int month = 1; 
			int year = 2016;
			testData.add(new DateTime(year, month, date, hour, minute));
		}
		
		DateTime dateTimeTrue = new DateTime("2016-01-01T10:30Z");
		DateTime dateTimeFalse = new DateTime("2016-01-01T18:50Z");
		assertTrue(qddt.detectTimeOfDay(testData, dateTimeTrue));
		assertFalse(qddt.detectTimeOfDay(testData, dateTimeFalse));
		
		qddt.close();
	}
}

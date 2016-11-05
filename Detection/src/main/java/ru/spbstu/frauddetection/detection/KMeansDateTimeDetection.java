package ru.spbstu.frauddetection.detection;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.joda.time.DateTime;

import ru.spbstu.frauddetection.FraudConfig.ObjectModel.*;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;

import org.joda.time.convert.ConverterManager;
import org.joda.time.convert.InstantConverter;
import org.joda.time.Chronology;

import java.io.IOException;

public class KMeansDateTimeDetection extends DetectionBaseSpark<DateTime> {
	
	private static enum FieldType{HOUR_OF_DAY, DAY_OF_WEEK, DAY_OF_MONTH, DAY_OF_YEAR, WEEK_OF_YEAR, MONTH_OF_YEAR};
	
	public KMeansDateTimeDetection() {
		super();
	}
	
	public KMeansDateTimeDetection(String master) {
		super(master);
	}
	
	public KMeansDateTimeDetection(JavaSparkContext ctx) {
		super(ctx);
	}
	
	private Function<DateTime,Integer> defineGetTime(FieldType fieldType) {
		Function<DateTime,Integer> getTime; 
		 
		switch(fieldType) {
		case HOUR_OF_DAY: {
			getTime = dt -> { return dt.getHourOfDay(); };
			break;
		}
		case DAY_OF_WEEK: {
			getTime = dt -> { return dt.getDayOfWeek(); };
			break;
		}
		case DAY_OF_MONTH: {
			getTime = dt -> { return dt.getDayOfMonth(); };
			break;
		}
		case DAY_OF_YEAR: {
			getTime = dt -> { return dt.getDayOfYear(); };
			break;
		}
		case WEEK_OF_YEAR: {
			getTime = dt -> { return dt.getWeekOfWeekyear(); };
			break;
		}
		case MONTH_OF_YEAR: {
			getTime = dt -> { return dt.getMonthOfYear(); };
			break;
		}
		default: {
			getTime = dt -> { return 0; };
			break;
		}
		}
		return getTime;
	}
	
	private Function<DateTime, Integer> defineGetCurrentPeriod(FieldType fieldType) {
		Function<DateTime,Integer> getCurrentPeriod; 
		 
		switch(fieldType) {
		case HOUR_OF_DAY: {
			getCurrentPeriod = dt -> { return dt.getDayOfWeek(); };
			break;
		}
		case DAY_OF_WEEK: {
			getCurrentPeriod = dt -> { return dt.getWeekOfWeekyear(); };
			break;
		}
		case DAY_OF_MONTH: {
			getCurrentPeriod = dt -> { return dt.getMonthOfYear(); };
			break;
		}
		case DAY_OF_YEAR: {
			getCurrentPeriod = dt -> { return dt.getYearOfCentury(); };
			break;
		}
		case WEEK_OF_YEAR: {
			getCurrentPeriod = dt -> { return dt.getYearOfCentury(); };
			break;
		}
		case MONTH_OF_YEAR: {
			getCurrentPeriod = dt -> { return dt.getYearOfCentury(); };
			break;
		}
		default: {
			getCurrentPeriod = dt -> { return 0; };
			break;
		}
		}
		return getCurrentPeriod;
	}
	
	private int defineLength(FieldType fieldType) {
		
		int lengthOfPeriod = 0;
		switch(fieldType) {
		case HOUR_OF_DAY: {
			lengthOfPeriod = 24;
			break;
		}
		case DAY_OF_WEEK: {
			lengthOfPeriod = 7;
			break;
		}
		case DAY_OF_MONTH: {
			lengthOfPeriod = 31;
			break;
		}
		case DAY_OF_YEAR: {
			lengthOfPeriod = 366;
			break;
		}
		case WEEK_OF_YEAR: {
			lengthOfPeriod = 53;
			break;
		}
		case MONTH_OF_YEAR: {
			lengthOfPeriod = 12;
			break;
		}
		default: {
			lengthOfPeriod = 0;
			break;
		}
		}
		return lengthOfPeriod;
	}
	
	private List<Vector> transposeMatrix(List<Vector> srcList, int dimension1) {
		List<Vector> newList = new ArrayList<Vector>();
		
		for (int i = 1; i <= dimension1; ++i) {
			double[] val = new double[srcList.size()];
			for (int j = 0; j < srcList.size(); ++j) {
				val[j] = srcList.get(j).toArray()[i]; 
			}
			newList.add(Vectors.dense(val));
		}
		return newList;
	}
	
	private int getMaxNum(List<Vector> srcList) {
		int maxLen = 0;
		int maxNum = 0;
		for (int i = 0; i < srcList.size(); ++i) {
			Vector curVec = srcList.get(i);
			int sum = 0;
			for (int j = 0; j < curVec.size(); ++j) {
				sum += curVec.apply(j);
			}
			if (sum > maxLen) {
				maxLen = sum;
				maxNum = i;
			}
		}
		return maxNum;
	}
	
	private Boolean isOK(List<DateTime> data, DateTime value, FieldType fieldType) {
		
		Function<DateTime, Integer> getTime = defineGetTime(fieldType);
		Function<DateTime, Integer> getCurrentPeriod = defineGetCurrentPeriod(fieldType);
		int lengthOfPeriod = defineLength(fieldType);
		
		List<DateTime> trainingList = data;
		trainingList.add(value);

		JavaRDD<DateTime> trainingRDD = sc.parallelize(trainingList);
		trainingRDD = trainingRDD.sortBy(
			dt -> {
				return dt.getMillis();
			}, true, trainingRDD.getNumPartitions());
		
		trainingList = trainingRDD.collect();

		double[] values = new double[lengthOfPeriod + 1];
		for (int i = 0; i < values.length; ++i) {
			values[i] = 0;
		}
		
		List<Vector> trainingVectorsList = new ArrayList<Vector>(); 
		int oldPeriod = 0;
		try {
			oldPeriod = getCurrentPeriod.call(trainingList.get(0));
		} catch (Exception e) {
			e.printStackTrace();
		}
		for (int i = 0; i < trainingList.size(); ++i) {
			int currentTime = 0;
			try {
				currentTime = getTime.call(trainingList.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
			int currentPeriod = 0;
			try {
				currentPeriod = getCurrentPeriod.call(trainingList.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (oldPeriod != currentPeriod) {
					trainingVectorsList.add(Vectors.dense(values));
				for (double item : values) {
					item = 0;
				}
				oldPeriod = currentPeriod;
			}
			values[currentTime]++;
		}
		trainingVectorsList.add(Vectors.dense(values));
		List<Vector> newTrainingVectorsList = transposeMatrix(trainingVectorsList, lengthOfPeriod);
		
		int maxNum = getMaxNum(newTrainingVectorsList);
		
		JavaRDD<Vector> trainingVectorsRDD = sc.parallelize(newTrainingVectorsList);
		trainingVectorsRDD.cache();
		
        int numClusters = 2;
        int numIterations = 100;
        
        KMeansModel clusters = KMeans.train(trainingVectorsRDD.rdd(), numClusters, numIterations);
        List<Integer> clusterNums = clusters.predict(trainingVectorsRDD).collect(); 
       
        int doubtNum = 0;
        try {
        	doubtNum = clusterNums.get(getTime.call(value) - 1);
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        return doubtNum == clusterNums.get(maxNum);
	}
	
	@Override
	public Boolean detect(List<DateTime> data, DateTime value) {
		
		boolean hourOfDay = isOK(data, value, FieldType.HOUR_OF_DAY);
		boolean dayOfWeek = isOK(data, value, FieldType.DAY_OF_WEEK);
		boolean dayOfMonth = isOK(data, value, FieldType.DAY_OF_MONTH);
		boolean dayOfYear = isOK(data, value, FieldType.DAY_OF_YEAR);
		boolean weekOfYear = isOK(data, value, FieldType.WEEK_OF_YEAR);
		boolean monthOfYear = isOK(data, value, FieldType.MONTH_OF_YEAR);
		
		return hourOfDay && dayOfWeek && dayOfMonth && dayOfYear && weekOfYear && monthOfYear;
	}
	
	@Override
	public List<DateTime> convertToType(InputGroup valueGroup, Group configGroup) {
		List<DateTime> converted = new ArrayList<DateTime>();

        for (InputType value : valueGroup.getValues()) {
            String valueName = value.getFieldName();
        	ConverterManager cm = ConverterManager.getInstance();

            for (Field field : configGroup.getFields()) {
                if (valueName.equals(field.getXpathName()) &&
                   (field.getType() == Type.Date)) {
                	try {
                		String valueToAdd = (String) value.getT();
                    	InstantConverter ic = cm.getInstantConverter(valueToAdd);
                    	Chronology chrono = ic.getChronology(valueToAdd, (Chronology) null);
                    	converted.add(new DateTime(ic.getInstantMillis(valueToAdd, chrono)));
                	} catch (ClassCastException e) {
                		e.printStackTrace();
                		System.out.println("Wrong type");
                	}
                }
            }
        }

        System.out.println("KMeans DateTime converted: ");
        System.out.println(converted);

        return converted;
	}
	
	@Override
	public List<List<DateTime>> convertToTypeList(List<InputGroup> data, Group configGroup) {
        List<List<DateTime>> convertedList = new ArrayList<>();

        for (InputGroup group : data) {
            convertedList.add(convertToType(group, configGroup));
        }

        return convertedList;
	}
}

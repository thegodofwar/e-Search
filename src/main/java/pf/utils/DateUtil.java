package pf.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	public static String genCurrentDayStr(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String dayStr = sdf.format(new Date());
		return dayStr;
	}
	
	
	public static String genCurrentHourStr(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String dayStr = sdf.format(new Date());
		return dayStr;
	}
	
	public static int genCurrentHour(){
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		String dayStr = sdf.format(new Date());
		return Integer.parseInt(dayStr);
	}
	
	public static void main(String[] args) {
		System.out.println(genCurrentDayStr());
		System.out.println(genCurrentHour());
	}

}

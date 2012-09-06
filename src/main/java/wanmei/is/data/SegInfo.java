package wanmei.is.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;


public class SegInfo {
	public static final Logger LOG=Logger.getLogger(SegInfo.class.getName());
	Date date = null;
	int seg = -1;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public SegInfo() {
		
	}
	
	public SegInfo(String dateStr, int seg) {
		
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
		this.seg = seg;
	}
	
	
	
	public String getDate() {
		if (date!= null) {
			return sdf.format(date);
		} else {
			return "";
		}
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public int getSeg() {
		return seg;
	}

	public void setSeg(int seg) {
		this.seg = seg;
	}

	public int compareTo(Object o) {
		SegInfo that = (SegInfo) o;
		if (this.date == null) {
			return -1;
		}
		if (that.date == null) {
			return 1;
		}
		int dateCom = this.date.compareTo(that.date);
		if (dateCom != 0) {
			return dateCom;
		} else {
			if (this.seg > that.seg) {
				return 1;
			} else if (this.seg == that.seg){
				return 0;
			} else {
				return -1;
			}
		}
		
	}
	

}

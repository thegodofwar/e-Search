package wanmei.utils;


import org.apache.log4j.Logger;

import wanmei.ls.LSUtils;

public class PartationUtil {
	public static final Logger LOG=Logger.getLogger(PartationUtil.class.getName());
	
	public static int getPartId(String usridStr, int partNo) {
		try {
			long userId = Long.parseLong(usridStr);
			//TODO:读取自定义配置文件
			return (int) userId % partNo;
		} catch (NumberFormatException e) {
			LOG.error("parse userid failed", e);
			return -1;
		}
	}
	
	public static int getIsId(String usridStr, int lsNum) {
		
		long userId = Long.parseLong(usridStr);
		int isId = (int) userId % lsNum; 
		if (isId < 0) {
			isId = 0-isId;
		}
		return isId;
		
	}
	
	public static void main(String[] args) {
		long l = -1582413630598134593L;
		System.out.println(getIsId(l+"", 1));
	}

}

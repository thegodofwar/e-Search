package wanmei.ls;


import wanmei.utils.PartationUtil;

public class LSUtils {

	public static int genPartId(String useridStr, int partNo) {
		/*try {
			long userId = Long.parseLong(useridStr);
			//TODO:读取自定义配置文件
			return (int) userId%partNo;
		} catch (NumberFormatException e) {
			LOG.log(Level.SEVERE, "parse userid failed", e);
			return -1;
		}*/
		
		return (int) Math.round(Math.random()*(partNo-1));
	}
	
	public static boolean checkLS(String usridStr, int slice, int lsNum) {
		int ls = PartationUtil.getIsId(usridStr, lsNum);
		
		return ls == slice;
	}
}

package pf.ls.search;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import pf.utils.XMLUtil;

public class LocalSearcherDaemonTool {
	
	public static final Logger LOG=Logger.getLogger(LocalSearcherDaemonTool.class.getName());
	
	
	public static void main(String[] args) {
		
		String majorPathStr=null;
		try {
			majorPathStr = XMLUtil.readValueStrByKey("ls_major_path");
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		String minorPathStr=null;
		try {
			minorPathStr = XMLUtil.readValueStrByKey("ls_minor_path");
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		int partNo=1;
		try {
			partNo = Integer.parseInt(XMLUtil.readValueStrByKey("partNo"));
		} catch (NumberFormatException e1) {
			LOG.error("",e1);
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		int slice=0;
		try {
			slice = Integer.parseInt(XMLUtil.readValueStrByKey("slice"));
		} catch (NumberFormatException e1) {
			LOG.error("",e1);
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		boolean loadToMem=false;
		try {
			loadToMem = Boolean.parseBoolean(XMLUtil.readValueStrByKey("loadToMem"));
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		String[] serverHosts=null;
		try {
			serverHosts = XMLUtil.readValueStrByKey("ds_ips").split(",");
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		
		int lsNum=1;
		try {
			lsNum = Integer.parseInt(XMLUtil.readValueStrByKey("isNum"));
		} catch (NumberFormatException e1) {
			LOG.error("",e1);
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		
		LocalSearcherDaemon daemon = new LocalSearcherDaemon();
		daemon.setLsNum(lsNum);
		try {
			daemon.init(partNo, majorPathStr, minorPathStr, slice, serverHosts, loadToMem);
		} catch (UnknownHostException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
		daemon.start();
		
	}

}

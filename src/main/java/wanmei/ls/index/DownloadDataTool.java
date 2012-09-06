package wanmei.ls.index;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import wanmei.is.tool.BuildMinorIndexTool;
import wanmei.is.tool.IssueMinorDataTool;
import wanmei.is.tool.UpdateMajorIndexTool;
import wanmei.utils.PathUtil;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.data.Segments;

public class DownloadDataTool {
	public static final Logger LOG=Logger.getLogger(DownloadDataTool.class.getName());
	public boolean exec(String baseIsPathStr, String baseLsPathStr) throws IOException {
		Path baseIsPath = new Path(baseIsPathStr);
		Path baseLsPath = new Path(baseLsPathStr);
		Path issueMajorPath = baseIsPath.cat(IssueMinorDataTool.ISSUE).cat(UpdateMajorIndexTool.MAJORINDEX);
		Path issueMinorPath = baseIsPath.cat(IssueMinorDataTool.ISSUE).cat(BuildMinorIndexTool.MINORINDEX);
		Path majorPath = baseLsPath.cat("major");
		Path minorPath = baseLsPath.cat("minor");
		
		Segments issueMajorSegss = new Segments(issueMajorPath);
		Segments issueMinorSegss = new Segments(issueMinorPath);
		String[] tag = {"issue"};
		
		List<Integer> issueMajorSegs = issueMajorSegss.findSegments(tag, null);
		List<Integer> issueMinorSegs = issueMinorSegss.findSegments(tag, null);
		
		Segments majors = new Segments(majorPath);
		Segments minors = new Segments(minorPath);
		
		List<Integer> majorSegs = majors.findSegments(new String[]{"merged"}, null);
		List<Integer> minorSegs = minors.findSegments(new String[]{"merged"}, null);
		
		int latestIssueMajor = getLatestSeg(issueMajorSegs);
		int latestIssueMinor = getLatestSeg(issueMinorSegs);
		int latestCurMajor = getLatestSeg(majorSegs);
		int latestCurMinor = getLatestSeg(minorSegs);
		
		if (latestIssueMajor > latestCurMajor) {
			copy(issueMajorPath.cat(latestIssueMajor+""), majorPath, latestIssueMajor);
			PathUtil.delete(new Path(baseIsPathStr).cat("MajorIsRunning"));
			
			//Only save the latest 3 major index folders(LS)!
			if(PathUtil.exists(majorPath.cat((latestIssueMajor-3)+""))) {
			   PathUtil.delete(majorPath.cat((latestIssueMajor-3)+""));
			}
			
			LOG.info("major stop");
		}
		if(latestIssueMinor==0&&latestCurMinor>0) {
			  for(int i=0;i<latestCurMinor;i++) {
				  PathUtil.delete(minorPath.cat(i+""));
			  }
			PathUtil.mkdirs(minorPath.cat(latestCurMinor+"").cat("LASTTAG"));
			copy(issueMinorPath.cat(latestIssueMinor+""),minorPath,latestIssueMinor);
			PathUtil.delete(new Path(baseIsPathStr).cat("MinorIsRunning"));
			LOG.info("minor stop");
		} else if(latestIssueMinor==0&&latestCurMinor==0) {//Running minor.sh at least twice,then running major.sh
			LOG.error("latestIssueMinor==0 and latestCurMinor==0");
			return false;
		} else if(latestIssueMinor==0&&latestCurMinor==-1) {
			copy(issueMinorPath.cat(latestIssueMinor+""),minorPath,latestIssueMinor);
			PathUtil.delete(new Path(baseIsPathStr).cat("MinorIsRunning"));
			LOG.info("minor stop");
		} else if(latestIssueMinor>0&&latestIssueMinor > latestCurMinor) {
			copy(issueMinorPath.cat(latestIssueMinor+""),minorPath,latestIssueMinor);
			PathUtil.delete(new Path(baseIsPathStr).cat("MinorIsRunning"));
			LOG.info("minor stop");
		}
		return true;
	}

	private void copy(Path src, Path dest, int ver) {
		Runtime rt = Runtime.getRuntime();
		if(!dest.asFile().exists()) {
			dest.asFile().mkdirs();
		}
		String cmd = "cp -rf "+ src.cat("index").getAbsolutePath() + " " + dest.cat(ver+"").getAbsolutePath();
		LOG.info("exec cmd:"+cmd);
		try {
			Process p = rt.exec(cmd);
			int rc = p.waitFor();
			LOG.info("exec return:"+rc);
		} catch (IOException e) {
			LOG.error("",e);
		} catch (InterruptedException e) {
			LOG.error("",e);
		}
		PathUtil.mkdirs(dest.cat(ver+"").cat("merged"));
		
	}

	private int getLatestSeg(List<Integer> issueMajorSegs) {
		if (issueMajorSegs != null && issueMajorSegs.size() > 0) {
			Collections.sort(issueMajorSegs);
			return issueMajorSegs.get(issueMajorSegs.size() - 1);
		} else {
			return -1;
		}
	}
	
	public static void main(String[] args) {
		DownloadDataTool tool = new DownloadDataTool();
			try {
				tool.exec(XMLUtil.readValueStrByKey("base_is_path"), XMLUtil.readValueStrByKey("base_ls_path"));
			} catch (Exception e) {
				LOG.error("",e);
			}
	}

}

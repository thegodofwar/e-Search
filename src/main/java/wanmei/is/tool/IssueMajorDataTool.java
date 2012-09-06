package wanmei.is.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import wanmei.is.data.SegInfo;
import wanmei.utils.MailConstants;
import wanmei.utils.PartationUtil;
import wanmei.utils.PathUtil;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.data.Segments;
import wanmei.utils.index.IndexBuilder;

/**
 * issue出来的目录：
 * issuepath - segNo - index - part - lucene_index
 *                   - tags
 * @author changwei
 *
 */
public class IssueMajorDataTool {
	public static final Logger LOG=Logger.getLogger(IssueMajorDataTool.class.getName());
	private int partNo;
	public static final int MAXMERGENO = 2560;
	
	public boolean exec(String basePath, int partNo ) throws InvalidPropertiesFormatException, IOException {
		class CheckMinorFinished implements Runnable {
			public CountDownLatch latch;
			public Path minorPathTag;
			CheckMinorFinished(CountDownLatch latch,Path minorPathTag) {
				 LOG.info("current-hour minor library issue step has not finished,waiting......");
				 this.latch=latch;
				 this.minorPathTag=minorPathTag;
			}
			public void run() {
				while(true) {
					if(!PathUtil.exists(minorPathTag)) {
						LOG.info("current-hour minor library issue step has finished!");
						latch.countDown();
						return;
					}
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						LOG.error("",e);
					}
				}
			}
		}
		if(PathUtil.exists(new Path(basePath).cat("MinorIsRunning"))) {
			CountDownLatch latch=new CountDownLatch(1);
			new Thread(new CheckMinorFinished(latch,new Path(basePath).cat("MinorIsRunning"))).start();
			try {
				latch.await();
			} catch (InterruptedException e) {
				LOG.error("",e);
			}
		}
		PathUtil.mkdirs(new Path(basePath).cat("MajorIsRunning"));
		LOG.info("major start");
		Path base = new Path(basePath);
		Path major = base.cat(IssueMinorDataTool.ISSUE).cat(UpdateMajorIndexTool.MAJORINDEX);
		Segments majorSegs = new Segments(major);
		int majorSeg = majorSegs.getLastSegment();
		Path majorPath = major.cat(""+majorSeg);
		if(!PathUtil.exists(majorPath.cat(MailConstants.ISSUE_TAG))) {
			LOG.error("The last major issue step failed because of missing "+majorPath.cat(MailConstants.ISSUE_TAG).toString());
			return false; 
		}
		if(!PathUtil.exists(majorPath.cat(MailConstants.MINORSEGFILE))) {
			LOG.error("The last major issue step failed because of missing "+majorPath.cat(MailConstants.MINORSEGFILE).toString());
		    return false;
		}
		
		Path newMajorPath = new Path(XMLUtil.readValueStrByKey("ls_major_path")).cat(""+majorSeg);
		if(majorSeg>-1&&!PathUtil.exists(newMajorPath.cat(MailConstants.MERGED_TAG))) {
			LOG.error("The last major issue step failed because of missing "+newMajorPath.cat(MailConstants.MERGED_TAG).toString());
			return false;
		}
		
		Path minor=base.cat(IssueMinorDataTool.ISSUE).cat(BuildMinorIndexTool.MINORINDEX);
		Segments minorSegs=new Segments(minor);
		int minorSeg=minorSegs.getLastSegment();
		Path minorPath=minor.cat(""+minorSeg);
        if(!PathUtil.exists(minorPath.cat(MailConstants.ISSUE_TAG))) {
        	LOG.error("The last minor issue step failed because of missing "+minorPath.cat(MailConstants.ISSUE_TAG).toString());
            return false;
        }
		if(!PathUtil.exists(minorPath.cat(MailConstants.MINORSEGFILE))) {
			LOG.error("The last minor issue step failed because of missing "+minorPath.cat(MailConstants.MINORSEGFILE).toString());
			return false;
		}
		
		Path newMinorPath = new Path(XMLUtil.readValueStrByKey("ls_minor_path")).cat(""+minorSeg);
		if(!PathUtil.exists(newMinorPath.cat(MailConstants.MERGED_TAG))) {
			LOG.error("The last minor issue step failed because of missing "+newMinorPath.cat(MailConstants.MERGED_TAG).toString());
			return false;
		}
		
		Path issuePath = base.cat(IssueMinorDataTool.ISSUE);
		Path issueMajorPath=issuePath.cat(UpdateMajorIndexTool.MAJORINDEX);
		Segments issueSegs = new Segments(issueMajorPath);

		this.partNo = partNo;
		
		//check major
		int segNo = issueSegs.createNewSegment();
		issueMajor(newMajorPath, newMinorPath,issueMajorPath.cat(segNo+"").cat("index"),majorSeg);
		SegInfo maxMinorVersion=XMLUtil.readSegFile(minorPath);
		XMLUtil.writeSegFile(maxMinorVersion, issueMajorPath.cat(segNo+""));
		PathUtil.mkdirs(issueMajorPath.cat(segNo+"").cat(MailConstants.ISSUE_TAG));
		
		//Only save the latest 3 major index folders(ISSUE)!
		if(PathUtil.exists(issueMajorPath.cat((segNo-3)+""))) {
		   PathUtil.delete(issueMajorPath.cat((segNo-3)+""));
		}
		
		return true;
	}	

	private void issueMajor(Path majorPath,Path minorPath,Path issuePath,int majorSeg) throws IOException {
		for (int i=0; i<partNo; i++) {
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(issuePath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
					new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
			indexWriter.setMaxMergeDocs(9999999);
	        indexWriter.setMergeFactor(300);
	        indexWriter.setMaxBufferedDocs(300);
	        if(majorSeg>-1) {
	        IndexReader majorIndexReader=IndexReader.open(FSDirectory.open(majorPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()));
	        indexWriter.addIndexes(majorIndexReader);
	        majorIndexReader.close();
	        }
	        IndexReader minorIndexReader=IndexReader.open(FSDirectory.open(minorPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()));
	        indexWriter.addIndexes(minorIndexReader);
	        minorIndexReader.close();
	        indexWriter.optimize();
	        indexWriter.close();
		}
	}	

	@SuppressWarnings("unused")
	private boolean check(Path inputPath) {
		
		
		if (PathUtil.exists(inputPath.cat(MailConstants.CANBEISSUE_TAG))) {
			return true;
		}
		return false;
	}
	
	public static void main(String[] args) {
		IssueMajorDataTool tool = new IssueMajorDataTool();
		
		String basePath=null;
		try {
			basePath = XMLUtil.readValueStrByKey("base_is_path");
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		int partNo=0;
		try {
			partNo = Integer.parseInt(XMLUtil.readValueStrByKey("partNo"));
		} catch (NumberFormatException e1) {
			LOG.error("",e1);
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		try {
			tool.exec(basePath, partNo);
		} catch (InvalidPropertiesFormatException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
	}

}

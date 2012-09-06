package pf.is.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import pf.is.data.SegInfo;
import pf.utils.MailConstants;
import pf.utils.PartationUtil;
import pf.utils.PathUtil;
import pf.utils.XMLUtil;
import pf.utils.data.Path;
import pf.utils.data.Segments;
import pf.utils.index.IndexBuilder;

/**
 * issue出来的目录：
 * issuepath - segNo - index - part - lucene_index
 *                   - tags
 * @author liufukun
 *
 */
public class IssueMinorDataTool {
	public static final Logger LOG=Logger.getLogger(IssueMinorDataTool.class.getName());
	private SegInfo majorVersion;
	private int partNo;
	public static final String ISSUE = "issue";
	
	public boolean exec(String basePath, int partNo ) throws InvalidPropertiesFormatException, IOException {
		Path base = new Path(basePath);
		Path minorPath = base.cat(BuildMinorIndexTool.INDEX).cat(BuildMinorIndexTool.MINORINDEX);
		Path major = base.cat(ISSUE).cat(UpdateMajorIndexTool.MAJORINDEX);
		Segments segs = new Segments(major);
		int seg = segs.getLastSegment();
		Path majorPath = major.cat(""+seg);
		if(seg==-1) {
			PathUtil.mkdirs(majorPath.cat(MailConstants.ISSUE_TAG));
		}

		Path issuePath = base.cat(ISSUE);
		Path issueMinorPath=issuePath.cat(BuildMinorIndexTool.MINORINDEX);
		Segments issueSegs = new Segments(issueMinorPath);
		
		majorVersion = XMLUtil.readSegFile(majorPath);
		this.partNo = partNo;
		
				
		
		//check minor
		List<Path> needIssueMinors = genIssueMinorInputs(minorPath);
		if (needIssueMinors != null && needIssueMinors.size() > 0) {
			int lastNo= issueSegs.getLastSegment();
			Path lastPath=issueMinorPath.cat(lastNo+"");
			if(!PathUtil.exists(lastPath.cat(MailConstants.MINORSEGFILE))&&lastNo!=-1) {
				LOG.error("The Last IssueMinor Process Failed:"+lastPath);
				return false;
			}
			
			SegInfo minorVersion=XMLUtil.readSegFile(lastPath);
			
			if(lastNo>0) {
				  SegInfo bMinorVersion=XMLUtil.readSegFile(issueMinorPath.cat((lastNo-1)+""));
				  if(minorVersion.compareTo(majorVersion)>0&&bMinorVersion.compareTo(majorVersion)<=0) {
				  	  LOG.error("versions conflict "+minorVersion.getDate()+","+minorVersion.getSeg()+" "+bMinorVersion.getDate()+","+bMinorVersion.getSeg());
				  	  return false;
				  }
				  if(lastNo>1) {
				  SegInfo bbMinorVersion=XMLUtil.readSegFile(issueMinorPath.cat((lastNo-2)+""));
				  if(minorVersion.compareTo(majorVersion)>0&&bbMinorVersion.compareTo(majorVersion)<=0) {
					  LOG.error("versions conflict "+minorVersion.getDate()+","+minorVersion.getSeg()+" "+bbMinorVersion.getDate()+","+bbMinorVersion.getSeg());
					  return false;
				  }
			    }
				}
			
			if(minorVersion.compareTo(majorVersion)<=0) {
				 for(int i=0;i<=lastNo;i++) {
				    PathUtil.delete(issueMinorPath.cat(i+""));
				 }
			}
			if(PathUtil.exists(issueMinorPath.cat(-1+""))) {
				PathUtil.delete(issueMinorPath.cat(-1+""));
			}
			int segNo = issueSegs.createNewSegment();
			issueMinors(needIssueMinors, issueMinorPath.cat(segNo+"").cat("index"),issueMinorPath,base,segNo);
			PathUtil.mkdirs(issueMinorPath.cat(segNo+"").cat(MailConstants.ISSUE_TAG));
		}
		
		return true;
	}

	private void issueMinors(List<Path> needIssueMinors, Path issuePath,Path issueMinorPath,Path base,int segNo) throws IOException {
		if(segNo>0) {
			for (int i=0; i<partNo; i++) {
				IndexWriter indexWriter = new IndexWriter(FSDirectory.open(issuePath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
						new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
				indexWriter.setMaxMergeDocs(9999999);
		        indexWriter.setMergeFactor(300);
		        indexWriter.setMaxBufferedDocs(300);
				List<IndexReader> mergeIndexs = new ArrayList<IndexReader>();
				for (Path minor : needIssueMinors) {
					Path[] minorUsrs = minor.listPathes();
					for (Path usrPath : minorUsrs) {
						if(usrPath.getName().matches(MailConstants.INDEXFOLDER+"\\d+")) {
						if (PartationUtil.getPartId(usrPath.getName().replaceAll(MailConstants.INDEXFOLDER, ""), partNo) == i ) {
							IndexReader reader = IndexReader.open(FSDirectory.open(usrPath.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()));
							mergeIndexs.add(reader);
						}
					}
					}
				}
					
			    indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
			    for (IndexReader ir : mergeIndexs) {
						ir.close();
				  }
			    Path updatedLsMinorPath=new Path(XMLUtil.readValueStrByKey("ls_minor_path"));
				Path lastPath=updatedLsMinorPath.cat((segNo-1)+"");
				IndexReader oldMinorIndex=IndexReader.open(FSDirectory.open(lastPath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()));
				indexWriter.addIndexes(oldMinorIndex);
				oldMinorIndex.close();
				indexWriter.optimize();
				indexWriter.close();
			}
			SegInfo maxCurrVersion=new SegInfo();
			Path minor = base.cat(BuildMinorIndexTool.INDEX).cat(BuildMinorIndexTool.MINORINDEX);
			if(!PathUtil.exists(minor.cat(MailConstants.MINORSEGFILE))) {
				LOG.error("The vesion-file "+minor.cat(MailConstants.MINORSEGFILE).toString()+" is missing,please check!");
				return;
			}
			maxCurrVersion = XMLUtil.readSegFile(minor);
			LOG.info("The version with date="+maxCurrVersion.getDate()+",seg="+maxCurrVersion.getSeg()+" issue successfully!");
			XMLUtil.writeSegFile(maxCurrVersion, base.cat(ISSUE).cat(BuildMinorIndexTool.MINORINDEX).cat(segNo+""));
			for(Path minorPathM : needIssueMinors) {
				PathUtil.delete(minorPathM);
			}
		} else if(segNo==0) {
			 for (int i=0; i<partNo; i++) {
					IndexWriter indexWriter = new IndexWriter(FSDirectory.open(issuePath.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
							new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
					indexWriter.setMaxMergeDocs(9999999);
			        indexWriter.setMergeFactor(300);
			        indexWriter.setMaxBufferedDocs(300);
					List<IndexReader> mergeIndexs = new ArrayList<IndexReader>();
					for (Path minor : needIssueMinors) {
						Path[] minorUsrs = minor.listPathes();
						for (Path usrPath : minorUsrs) {
							if(usrPath.getName().matches(MailConstants.INDEXFOLDER+"\\d+")) {
							if (PartationUtil.getPartId(usrPath.getName().replaceAll(MailConstants.INDEXFOLDER, ""), partNo) == i ) {
								IndexReader reader = IndexReader.open(FSDirectory.open(usrPath.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()) );
								mergeIndexs.add(reader);
							}
						}
						}
					}
						
				   indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
				   for (IndexReader ir : mergeIndexs) {
							ir.close();
				   }
					indexWriter.optimize();
					indexWriter.close();
				}
			    SegInfo maxCurrVersion=new SegInfo();
				Path minor = base.cat(BuildMinorIndexTool.INDEX).cat(BuildMinorIndexTool.MINORINDEX);
				if(!PathUtil.exists(minor.cat(MailConstants.MINORSEGFILE))) {
					LOG.error("The vesion-file "+minor.cat(MailConstants.MINORSEGFILE).toString()+" is missing,please check!");
					return;
				}
				maxCurrVersion = XMLUtil.readSegFile(minor);
				LOG.info("The version with date="+maxCurrVersion.getDate()+",seg="+maxCurrVersion.getSeg()+" issue successfully!");
				XMLUtil.writeSegFile(maxCurrVersion, base.cat(ISSUE).cat(BuildMinorIndexTool.MINORINDEX).cat(segNo+""));
				for(Path minorPathM : needIssueMinors) {
				  PathUtil.delete(minorPathM);
			}
		}
	}
	
	private List<Path> genIssueMinorInputs(Path minorPath) throws IOException {
		
		List<Path> issueMinors = new ArrayList<Path>();
		
		Segments segs = new Segments(minorPath);
		String[] builtTag = {MailConstants.CANBEISSUE_TAG};
		List<Integer> segNos = segs.findSegments(builtTag, null);
		if(!PathUtil.exists(minorPath.cat(MailConstants.MINORSEGFILE))) {
			LOG.error("ERROR: The "+minorPath.toString()+"/"+MailConstants.MINORSEGFILE+" is missing,please check!");
			return null;
		}
		SegInfo version = XMLUtil.readSegFile(minorPath);
		for (int seg : segNos) {
			Path minor = minorPath.cat(seg+"");
			LOG.info("major: "+majorVersion.getDate()+","+majorVersion.getSeg()+"  "+ "minor:"+version.getDate()+","+version.getSeg());
			if (version.compareTo(majorVersion) > 0) {
				LOG.info("add minor:"+version.getDate()+","+version.getSeg());
				issueMinors.add(minor);
			} else {
				LOG.error("ERROR: The "+minorPath.toString()+"/"+MailConstants.MINORSEGFILE+"'s version is not higher than major version!");
				//PathUtil.delete(minor);
			}
		}
		return issueMinors;
	}
	
	public static void main(String[] args) {
		IssueMinorDataTool tool = new IssueMinorDataTool();
		
		String basePath=null;
		try {
			basePath = XMLUtil.readValueStrByKey("base_is_path");
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
		try {
			tool.exec(basePath, partNo);
		} catch (InvalidPropertiesFormatException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
	}
}

package wanmei.is.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class UpdateMajorIndexTool {
	
	public static final Logger LOG=Logger.getLogger(UpdateMajorIndexTool.class.getName());
	public static final String MAJORINDEX = "major";
	public static final String MINORINDEX = "minor";
	
	private boolean isUsrPath(Path path) {
		if (path.asFile().isDirectory() && !path.getName().equals(MailConstants.CANBEISSUE_TAG)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean exec(String basePathStr) throws IOException {
		
		Path basePath = new Path(basePathStr);
		
		Path majorPath = basePath.cat(BuildMinorIndexTool.INDEX).cat(MAJORINDEX);
		Path minorPath = basePath.cat(BuildMinorIndexTool.INDEX).cat(MINORINDEX);
		
		Segments segs = new Segments(majorPath);
		int newseg = segs.createNewSegment();
		int lastseg = newseg -1;
		
		Path major = majorPath.cat(""+lastseg);
		Path indexPath = majorPath.cat(""+newseg);
		
		
		try {
			List<Path> minors = genMinorInputs(minorPath, major);
			Map<String, List<Path>> majorMap = new HashMap<String, List<Path>>();
			Path[] majorUsrs = major.listPathes();
			for (Path majorUsr : majorUsrs) {
				if (isUsrPath(majorUsr)) {
					if (majorMap.containsKey(majorUsr.getName())) {
						majorMap.get(majorUsr.getName()).add(majorUsr);
					} else {
						List<Path> list = new ArrayList<Path>();
						list.add(majorUsr);
						majorMap.put(majorUsr.getName(), list);
					}
				}
			}
			
			
			
			
			SegInfo maxVersion = XMLUtil.readSegFile(major);
			
			if (minors != null && minors.size() > 0) {

				for (Path minor : minors) {
					Path[] usrs = minor.listPathes();
					for (Path usr : usrs) {
						if (usr.asFile().isDirectory() && !usr.getName().equals(MailConstants.CANBEISSUE_TAG)) {
							if (majorMap.containsKey(usr.getName())) {
								majorMap.get(usr.getName()).add(usr);
							} else {
								List<Path> list = new ArrayList<Path>();
								list.add(usr);
								majorMap.put(usr.getName(), list);
							}
						}
					}
					
					SegInfo version = XMLUtil.readSegFile(minor);
					if (version.compareTo(maxVersion) > 0) {
						maxVersion = version;
					}
					
				}
				
				
				
				for (String usrname : majorMap.keySet()) {
					
					int count = 0;
					List<IndexReader> mergeIndexs = new ArrayList<IndexReader>();
					IndexWriter indexWriter = new IndexWriter(FSDirectory.open(indexPath.cat(usrname).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
							new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
					indexWriter.setMaxMergeDocs(1024);
			        indexWriter.setMergeFactor(100);
					for (Path mi : majorMap.get(usrname)) {
						try {
							IndexReader reader = IndexReader.open(FSDirectory.open(mi.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()) );
							
							mergeIndexs.add(reader);
							count++;
						} catch (Exception e) {
							LOG.info("stop at "+count);
							LOG.error("",e);
						}
						
						if (count > IssueMajorDataTool.MAXMERGENO) {
							
							
								indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
							
							
							for (IndexReader ir : mergeIndexs) {
								ir.close();
							}
							
							mergeIndexs.clear();
							count = 0;
						}
						
					}
					
					if (mergeIndexs.size() > 0) {
				        indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
				        for (IndexReader ir : mergeIndexs) {
							ir.close();
						}
				        mergeIndexs.clear();
					}
					
					indexWriter.close();
					
				}
				XMLUtil.writeSegFile(maxVersion, indexPath);
				PathUtil.mkdirs(indexPath.cat(MailConstants.CANBEISSUE_TAG));
				
				int delete = newseg -2 ;
				if (PathUtil.exists(majorPath.cat(""+delete)) ) {
					PathUtil.delete(majorPath.cat(""+delete));
				}
				
				return true;
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
		return false;
		/*//TODO:add & del indexes
		Map<String, List<Path>> addInputs = new HashMap<String, List<Path>>();
		Map<String, List<Path>> delInputs = new HashMap<String, List<Path>>();
		SegInfo infoStr = new SegInfo();
		int count = 0;
		try {
			
			infoStr = genInputFiles(addPath, addInputs, true);
			genInputFiles(delPath, delInputs, false);
			
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "gen input failed", e);
			return false;
		}
		
		//process add input segment
		for (String usrIdStr : addInputs.keySet()) {
			// init index builder
			Path indexDir = tmpPath.cat(usrIdStr);
			IndexBuilder builder = new IndexBuilder(indexDir);
			try {
				builder.prepare();
			} catch (CorruptIndexException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			} catch (LockObtainFailedException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			}
			List<Path> usrDatas = addInputs.get(usrIdStr);
			for (Path usrData : usrDatas) {
				List<MailMeta> metas = XMLUtil.loadMailMetaFromXml(usrData.asFile());
				for (MailMeta meta : metas) {
					builder.setIndexInfo(meta.getId(), meta);
					try {
						builder.index();
						count++;
					} catch (IOException e) {
						LOG.log(Level.INFO, "build index fail:"+meta.getId(), e);
					}
				}
			}
			try {
				builder.finish();
				
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "finish index fail:"+usrIdStr, e);
				return false;
			}
		}// for
		
		LOG.info(count + " files indexed, usring "+((System.currentTimeMillis() - start)/1000) + "s");
		
		for (String usrIdStr : delInputs.keySet()) {
			// init index builder
			Path indexDir = indexPath.cat(usrIdStr);
			IndexBuilder builder = new IndexBuilder(indexDir);
			try {
				builder.prepare();
			} catch (CorruptIndexException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			} catch (LockObtainFailedException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "indexBuilder prepare failed", e);
				return false;
			}
			List<Path> usrDatas = addInputs.get(usrIdStr);
			for (Path usrData : usrDatas) {
				List<Long> ids = XMLUtil.loadMailIdFromXml(usrData.asFile());
				for (Long id : ids) {
					try {
						builder.deleteIndexById(id);
					} catch (IOException e) {
						LOG.log(Level.INFO, "build index fail:"+id, e);
					}
				}
			}
			try {
				builder.finish();
			} catch (IOException e) {
				LOG.log(Level.SEVERE, "finish index fail:"+usrIdStr, e);
				return false;
			}
		}
		
		
		try {
			removeOldDirs(addInputs.values());
			removeOldDirs(delInputs.values());
			XMLUtil.writeSegFile(infoStr, indexPath);
			PathUtil.mkdirs(indexPath.cat(MailConstants.CANBEISSUE_TAG));
			
		} catch (IOException e) {
			LOG.log(Level.SEVERE, "remove old data failed", e);
			return false;
		}
		
		
		return true;*/
	}
	
	/*private SegInfo genInputFiles(Path segmentBase, Map<String, List<Path> > needIndexSegs, boolean isadd) throws IOException {
		System.out.println("inputseg:"+segmentBase.getAbsolutePath());
		Path[] days = segmentBase.listPathes();
		
		SegInfo infoStr = new SegInfo();
		
		for (Path day : days) {
			//获取应该index的day文件夹
			
				Segments seg = new Segments(day);
				String[] builtTag = {MailConstants.BUILT_TAG};
				if (!isadd) {
					builtTag = null;
				}
				List<Integer> segNos = seg.findSegments(builtTag, null);
				
				for (int no : segNos) {
					SegInfo info = new SegInfo(day.getName(), no);
					
					Path[] users = day.cat(no+"").listPathes();
					for (Path usr : users) {
						if (!usr.asFile().isFile()) {
							//不是文件,则为tag，不处理
							continue;
						}
						String usrname = usr.getName().split("_")[0];
						if (needIndexSegs.containsKey(usrname)) {
							needIndexSegs.get(usrname).add(usr);
						} else {
							List<Path> newusr = new ArrayList<Path>();
							newusr.add(usr);
							needIndexSegs.put(usrname, newusr);
						}
						if (info.compareTo(infoStr)>0) {
							infoStr = info;
						}
					}
				}
			
		}
		return infoStr;
	}*/
	
	private void mergeMinors(List<Path> needIssueMinors, Path mergePath) throws IOException {
		int count = 0;
		
		
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(mergePath.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
					new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
			indexWriter.setMaxMergeDocs(1024);
	        indexWriter.setMergeFactor(100);
			List<IndexReader> mergeIndexs = new ArrayList<IndexReader>();
			for (Path minor : needIssueMinors) {
				Path[] minorUsrs = minor.listPathes();
				for (Path usrPath : minorUsrs) {
					
					IndexReader reader = IndexReader.open(FSDirectory.open(usrPath.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()) );
					mergeIndexs.add(reader);
					
					count++;
					
					if (count > IssueMajorDataTool.MAXMERGENO) {
						indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
						for (IndexReader ir : mergeIndexs) {
							ir.close();
						}
						mergeIndexs.clear();
						count = 0;
					}
					
				}
			}
			if (mergeIndexs.size() > 0) {
				
		        indexWriter.addIndexes(mergeIndexs.toArray(new IndexReader[mergeIndexs.size()]));
		        
			}
			indexWriter.close();
		
	}
	
	private List<Path> genMinorInputs(Path minorPath, Path majorPath) throws IOException {
		SegInfo majorVersion = XMLUtil.readSegFile(majorPath);
		List<Path> issueMinors = new ArrayList<Path>();
		
		Segments segs = new Segments(minorPath);
		String[] builtTag = {MailConstants.CANBEISSUE_TAG};
		List<Integer> segNos = segs.findSegments(builtTag, null);
		
		for (int seg : segNos) {
			Path minor = minorPath.cat(seg+"");
			SegInfo version = XMLUtil.readSegFile(minor);
			LOG.info("major: "+majorVersion + "minor:"+version);
			if (version.compareTo(majorVersion) > 0) {
				LOG.info("add minor:"+version);
				issueMinors.add(minor);
				
			} else {
				PathUtil.delete(minor);
			}
		}
		return issueMinors;
	}
	
	/*private void removeOldDirs(Collection<List<Path>> segmentPath) throws IOException {
		
		for (List<Path> usr : segmentPath) {
			for (Path path : usr) {
				PathUtil.delete(path);
			}
		}
		
	}*/
	
	public static void main(String[] args) {
		UpdateMajorIndexTool tool = new UpdateMajorIndexTool();
		
		String basePath=null;
		try {
			basePath = XMLUtil.readValueStrByKey("base_is_path");
		} catch (Exception e1) {
			LOG.error("",e1);
		}
		try {
			tool.exec(basePath);
		} catch (IOException e) {
			LOG.error("",e);
		}
		
		
		
		
		
	}

}

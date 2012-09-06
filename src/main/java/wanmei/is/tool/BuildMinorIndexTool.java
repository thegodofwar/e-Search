package wanmei.is.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

import wanmei.is.data.MailMeta;
import wanmei.is.data.SegInfo;
import wanmei.utils.MailConstants;
import wanmei.utils.PathUtil;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.data.Segments;
import wanmei.utils.index.IndexBuilder;

public class BuildMinorIndexTool {
	
	public static final Logger LOG=Logger.getLogger(BuildMinorIndexTool.class.getName());
	public static final String MINORINDEX = "minor";
	public static final String INDEX = "index";
	
	
	public boolean exec(String basePathStr) throws IOException{
		int count = 0;
		long start = System.currentTimeMillis();
	    Path basePath = new Path(basePathStr);
		
		Path segmentBase = basePath.cat(MailDataParseTool.SEGMENT).cat(MailDataParseTool.ADD);
		LOG.info("segment:" + segmentBase.getAbsolutePath());
		Path[] days = segmentBase.listPathes();
		for (Path day : days) {
			
			
			Segments inputSeg = new Segments(day);
			String[] builtTag = {MailConstants.BUILT_TAG};
			String[] finishTag = {MailConstants.SEGFINISHED};
			List<Integer> segNos = inputSeg.findSegments(finishTag, builtTag);
			if(segNos.size()==0) {
				continue;
			}
			LOG.info("total segments:"+segNos.size());
			
			
			//产生输出segment
			Path outputDir = basePath.cat(INDEX).cat(MINORINDEX);
			Segments seg = new Segments(outputDir);
			int segNo = -1;
			try {
				segNo = seg.createNewSegment();
			} catch (IOException e) {
				LOG.error("create segment failed", e);
				return false;
			}
			Path outputSeg = outputDir.cat(segNo+"");
			
			int maxNo = 0;
			Map<String, List<Path>> inputs =  new HashMap<String, List<Path>>();
			
			for (int no : segNos) {

				if (no > maxNo) {
					maxNo = no;
				}
				
				try {
					Map<String, List<Path>> newinputs = genInputFiles(day.cat(no+""));
					inputs = addMap(inputs, newinputs);
				} catch (IOException e) {
					LOG.error("gen input failed", e);
					return false;
				}
				mkBuiltTags(day, no);
			}
		
			if (inputs == null){
				LOG.info("inputs null");
				break;
			} else {
				count = 0;
				for (String usrIdStr : inputs.keySet()) {
					
					// init index builder
					Path indexDir = outputSeg.cat(usrIdStr);
					IndexBuilder builder = new IndexBuilder(indexDir);
					try {
						builder.prepare();
					} catch (CorruptIndexException e) {
						LOG.error("indexBuilder prepare failed", e);
						return false;
					} catch (LockObtainFailedException e) {
						LOG.error("indexBuilder prepare failed", e);
						return false;
					} catch (IOException e) {
						LOG.error("indexBuilder prepare failed", e);
						return false;
					}
					List<Path> usrDatas = inputs.get(usrIdStr);
					for (Path usrData : usrDatas) {
						List<MailMeta> metas = XMLUtil.loadMailMetaFromXml(usrData.asFile());
						for (MailMeta meta : metas) {
							builder.setIndexInfo(meta.getDocid(), meta);
							try {
								builder.index();
								count++;
							} catch (Exception e) {
								LOG.error("build index fail:"+meta.getId(), e);
							}
							
						}
					}
					try {
						builder.finish();
						//write tag, for download
						
					} catch (IOException e) {
						LOG.error("finish index fail:"+usrIdStr, e);
						return false;
					}
				}
				
				LOG.info(day.getName()+" "+count +" indexs made, using "+((System.currentTimeMillis() - start)/1000) + "s");
			}
				
			
			PathUtil.mkdirs(outputSeg.cat(MailConstants.CANBEISSUE_TAG));
			XMLUtil.writeSegFile(new SegInfo(day.getName(),maxNo), outputSeg);
		}
		return true;
	}	

	private Map<String, List<Path> > genInputFiles(Path segmentPath) throws IOException {
		
		Map<String, List<Path>> needIndexSegs = new HashMap<String, List<Path>>();
		
		Path[] users = segmentPath.listPathes();
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
			
		}
		return needIndexSegs;
	}
	
	
	private void mkBuiltTags(Path day, int no) throws IOException {
		
		/*Path[] days = segmentBase.listPathes();		
		for (Path day : days) {
			//获取应该index的day文件夹
//			if (! PathUtil.exists(day.cat(MailConstants.DAY_FINISHED)) ) {
			Segments seg = new Segments(day);
			String[] builtTag = {MailConstants.BUILT_TAG};
			List<Integer> segNos = seg.findSegments(null, builtTag);
			for (int no : segNos) {*/
				Path tag = day.cat(no+"").cat(MailConstants.BUILT_TAG);
				PathUtil.mkdirs(tag);
			/*}
				//创建Finished tag,segment有新数据写入后删除此tag
//				PathUtil.mkdirs(day.cat(MailConstants.DAY_FINISHED));
//			}
		}*/
		
	}
	
	public static Map<String, List<Path>> addMap(Map<String, List<Path>> a, Map<String, List<Path>> b ) {
		for (String key : b.keySet()) {
			if (a.containsKey(key)) {
				a.get(key).addAll(b.get(key));
			} else {
				a.put(key, b.get(key));
			}
		}
		return a;
	}
	
	public static void main(String[] args) {
		BuildMinorIndexTool tool = new BuildMinorIndexTool();
		
		String dataPath=null;
		try {
			dataPath = XMLUtil.readValueStrByKey("base_is_path");
		} catch (Exception e1) {
			LOG.info("",e1);
		}
		
		try {
			tool.exec(dataPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
		
		
	}
	

}

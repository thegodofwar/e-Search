package wanmei.ls.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import wanmei.utils.MailConstants;
import wanmei.utils.PathUtil;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.data.Segments;
import wanmei.utils.index.IndexBuilder;

/*
 * major/seg/indexdata-part
 * minor/day/seg/indexdata-part & seginfo.xml
 * TODO:remove old major 和minor在localsearcherDaemon中做。
 */
public class MergeMajorIndexTool {
	public static final Logger LOG=Logger.getLogger(MergeMajorIndexTool.class.getName());
	public boolean exec(String minorPathStr, String majorPathStr, int partNo) throws IOException {
		Path minorPath = new Path(minorPathStr);
		Path majorPath = new Path(majorPathStr);
		
		Path[] days = minorPath.listPathes();
		List<Path> mergedArray = new ArrayList<Path>();
		
		Segments majorSeg = new Segments(majorPath);
		
		List<Integer> majorlist = majorSeg.findSegments(null, null);
		int lastMajor = -1;
		//find last major no
		for (Integer majorNo : majorlist) {
			if (majorNo > lastMajor) {
				lastMajor = majorNo;
			}
		}
		
		
		if (lastMajor >= 0) {
			mergedArray.add(majorPath.cat(lastMajor+""));
		}
		
		int maxVersion = -1;
		for (Path day : days) {
			if (PathUtil.exists(day.cat(MailConstants.DAY_FINISHED))) {
				Segments minorSeg = new Segments(day);
				int lastseg = minorSeg.getLastSegment();
				if (lastseg >= 0) {
					mergedArray.add(day.cat(lastseg+""));
				}
				if (lastseg > maxVersion) {
					maxVersion = lastseg;
				}
			}
		}
		if (maxVersion < 0) {
			//TODO:log
			return false;
		}
		
		Path output = majorPath.cat(maxVersion + "");
		mergeIndexByPart(mergedArray, output, partNo);
		PathUtil.mkdirs(output.cat(MailConstants.MERGED_TAG));
		return true;
	}
	
	
	private void mergeIndexByPart(List<Path> mergedArray, Path output,
			int partNo) throws CorruptIndexException, IOException {
		 
		for (int i=0; i<partNo; i++) {
			IndexReader[] mergeinputs = new IndexReader[mergedArray.size()];
			for (int j=0; j<mergedArray.size(); j++) {
				mergeinputs[j] = IndexReader.open(FSDirectory.open(mergedArray.get(j).cat("part-"+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()));
			}
			IndexWriter indexWriter = new IndexWriter(FSDirectory.open(output.cat(MailConstants.PART_PRE+i).cat(IndexBuilder.LUCENE_INDEX_DIR).asFile()), 
					new IKAnalyzer(true), true, IndexWriter.MaxFieldLength.LIMITED);
			indexWriter.setMaxMergeDocs(1024);
	        indexWriter.setMergeFactor(100);
			indexWriter.addIndexes(mergeinputs);
			indexWriter.close();
		}
		
	}


	public static void main(String[] args) {
		MergeMajorIndexTool tool = new MergeMajorIndexTool();
		String minorPathStr = args[0];
		String majorPathStr = args[1];
		int partNo = Integer.parseInt(args[2]);
		try {
			tool.exec(minorPathStr, majorPathStr, partNo);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
		
	}

}

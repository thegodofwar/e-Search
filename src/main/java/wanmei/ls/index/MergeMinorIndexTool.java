package wanmei.ls.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

import wanmei.ls.LSUtils;
import wanmei.utils.DateUtil;
import wanmei.utils.MailConstants;
import wanmei.utils.PathUtil;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.data.Segments;
import wanmei.utils.index.IndexBuilder;

/*
 * Merge下载的数据和minor库，产生新的Minor库
 * 数据由IS分发，产生以后由脚本直接直接分发到LS
 * localMinorPath/day/seg/partid/lucen-index/index
 * localMinorPath/day/seg/seginfo.xml
 * dataPath/seg/usrid/index
 * 
 */
public class MergeMinorIndexTool {
	public static final Logger LOG=Logger.getLogger(MergeMinorIndexTool.class.getName());
	public static final String LOCAL_DIST_VER = "local-dist-ver";

	/**
	 * merge downloaded index data into minor index base, make new segment and
	 * tag it.
	 * 
	 * @param localMinor
	 *            the minor path str of local minor index base
	 * @param dataPathStr
	 *            path str of index downloaded from IS
	 * @param partNo
	 *            index part numbers
	 * @return true if success, false otherwise
	 * @throws Exception
	 *             if any error occurs
	 */
	public boolean exec(String localMinor, String dataPathStr, int partNo)
			throws ParseException {
		Path localPath = new Path(localMinor);
		Path dataPath = new Path(dataPathStr);
		Path[] localdays = localPath.listPathes();

		
		String dayStr = DateUtil.genCurrentDayStr();

		Path dayMinorPath = localPath.cat(dayStr);
		PathUtil.mkdirs(dayMinorPath);
		Segments seg = new Segments(dayMinorPath);
		int lastseg = -1;
		try {
			lastseg = seg.getLastSegment();
		} catch (IOException e) {
			LOG.error("seg error", e);
			return false;
		}
		try {
			if (lastseg >= 0) {
				merge(dataPath, dayMinorPath, dayMinorPath.cat("" + lastseg),
						partNo, dayMinorPath.cat(MailConstants.MINORSEGFILE).asFile());
			} else {
				merge(dataPath, dayMinorPath, null, partNo, dayMinorPath.cat(MailConstants.MINORSEGFILE)
						.asFile());
			}

		} catch (InvalidPropertiesFormatException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}

		for (Path localDay : localdays) {
			PathUtil.mkdirs(localDay.cat(MailConstants.DAY_FINISHED));
		}

		return true;
	}

	public boolean merge(Path inputPath, Path outputPath, Path localInputPath,
			int partNo, File segInfoFile) throws InvalidPropertiesFormatException, IOException {
		// 读取minor库中版本信息
		int localIndexVer = -1;
		if (localInputPath != null && PathUtil.exists(localInputPath)) {
			 
			InputStream in = null;
			try {
				in = new FileInputStream(segInfoFile);
			} catch (FileNotFoundException e) {
				LOG.error("SegInfoFile missed", e);
				return false;
			}

			Properties prop = new Properties();
			try {
				prop.loadFromXML(in);
			} finally {
				in.close();
			}
			localIndexVer = Integer.parseInt(prop.getProperty(LOCAL_DIST_VER,
					"-1"));
		}

		Segments seg = new Segments(inputPath);
		int maxVersion = seg.getLastSegment();
		if (localIndexVer >= maxVersion) {
			// do nothing
			return false;
		} else {
			long mergeStartTime = System.currentTimeMillis();
			for (int i = 0; i < partNo; i++) {
				mergeIndexByPart(localInputPath, inputPath, outputPath
						.cat(maxVersion + ""), i, localIndexVer, maxVersion,
						partNo);
			}
			long mergeEndTime = System.currentTimeMillis();
			//TODO:
//			XMLUtil.writeSegFile(maxVersion + "", outputPath);
			PathUtil.mkdirs(outputPath.cat(maxVersion + "").cat(
					MailConstants.MERGED_TAG));
			LOG.info("merge index use " + (mergeEndTime - mergeStartTime)
					/ 1000 + " s.");
		}

		return true;
	}

	private void mergeIndexByPart(Path minorPath, Path inputPath,
			Path outputPath, int partId, int localIndexVer, int maxVersion,
			int partNo) throws CorruptIndexException, IOException {
		List<IndexReader> mergeIndexArray = new ArrayList<IndexReader>();
		if (minorPath != null && PathUtil.exists(minorPath)) {
			if (PathUtil.exists(minorPath.cat(partId + ""))) {
				mergeIndexArray.add(IndexReader.open(FSDirectory.open(minorPath
						.cat(MailConstants.PART_PRE + partId).cat(
								IndexBuilder.LUCENE_INDEX_DIR).asFile())));
			}
		}

		for (int i = localIndexVer + 1; i <= maxVersion; i++) {
			Path segPath = inputPath.cat(i + "");
			Path[] userPathes = segPath.listPathes();
			for (Path userPath : userPathes) {
				if (!userPath.getName().equals("built")){
					int shouldInPart = LSUtils
							.genPartId(userPath.getName(), partNo);
					if (PathUtil.exists(segPath) && shouldInPart == partId) {
						mergeIndexArray.add(IndexReader.open(FSDirectory
								.open(userPath.cat(IndexBuilder.LUCENE_INDEX_DIR).asFile())));
					}
				}
			}
		}
		IndexWriter indexWriter = new IndexWriter(FSDirectory.open(outputPath
				.cat(MailConstants.PART_PRE + partId).cat(
						IndexBuilder.LUCENE_INDEX_DIR).asFile()),
				new IKAnalyzer(true), true, IndexWriter.MaxFieldLength.LIMITED);
		indexWriter.setMaxMergeDocs(1024);
		indexWriter.setMergeFactor(100);
		indexWriter.addIndexes(mergeIndexArray.toArray(new IndexReader[0]));
		indexWriter.close();

	}

	public static void main(String[] args) {
		MergeMinorIndexTool tool = new MergeMinorIndexTool();
		String localMinor = args[0];
		String dataPath = args[1];
		int partNo = Integer.parseInt(args[2]);
		try {
			tool.exec(localMinor, dataPath, partNo);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			LOG.error("",e);
		}
		
		

	}

}

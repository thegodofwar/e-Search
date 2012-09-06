package pf.utils;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.FSDirectory;

import pf.utils.data.Path;

public class SegmentUtil {
	public static final Logger LOG=Logger.getLogger(SegmentUtil.class.getName());
	
	public static void getSegmentsCount(Path... segmentPaths) {
		IndexSearcher indexSearchers[]=new IndexSearcher[segmentPaths.length];
		for(int i=0;i<segmentPaths.length;i++) {
		   IndexReader reader=null;
		    try {
		      reader=IndexReader.open(FSDirectory.open(segmentPaths[i].asFile(),null));
		     } catch (CorruptIndexException e) {
			   LOG.error("",e);
		     } catch (IOException e) {
			   LOG.error("",e);
		  }
		   indexSearchers[i]=new IndexSearcher(reader);
		}
		MultiSearcher mulSearcher=null;
		try {
			 mulSearcher=new MultiSearcher(indexSearchers);
		} catch (IOException e) {
			LOG.error("",e);
		}
		BooleanQuery q=new BooleanQuery();
		TermQuery hasAttQ=new TermQuery(new Term(MailConstants.HASATT_FIELD,"1"));
		TermQuery noAttQ=new TermQuery(new Term(MailConstants.HASATT_FIELD,"2"));
		q.add(hasAttQ,Occur.SHOULD);
		q.add(noAttQ,Occur.SHOULD);
		Sort sort=new Sort(new SortField(MailConstants.TIME_FIELD, SortField.STRING, true));
		TopFieldDocs docs=null;
		try {
			 docs = mulSearcher.search(q, null, 500000, sort);
		} catch (IOException e) {
			LOG.error("",e);
		}
		Path result=new Path("/mailsearch-branch20110816/run/").cat("result");
		FileWriter fw=null;
		try {
		  fw=new FileWriter(result.asFile());
		} catch (IOException e) {
			LOG.error("",e);
		}
		try {
			LOG.info("Total index counts:"+docs.totalHits);
			fw.write("Total index counts:"+docs.totalHits);
		} catch (IOException e) {
			LOG.error("",e);
		}
		try {
			fw.close();
		} catch (IOException e) {
			LOG.error("",e);
		}
	}
	
	public static void main(String args[]) {
		getSegmentsCount(new Path("/data2/ls/major/7/part-0/lucene_index/"),new Path("/data2/ls/minor/0/part-0/lucene_index/"));
	}
	
}

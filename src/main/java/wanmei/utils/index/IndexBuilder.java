package wanmei.utils.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.wltea.analyzer.lucene.IKAnalyzer;

import wanmei.is.data.MailMeta;
import wanmei.is.tool.BuildMinorIndexTool;
import wanmei.ls.search.LocalSearcherDaemon;
import wanmei.utils.MailConstants;
import wanmei.utils.data.Path;

public class IndexBuilder {
	public static final String LUCENE_INDEX_DIR = "lucene_index";
	public final static String TAG_INDEX_DOC_ID = "doc_id";
	public static final String INDEX_PRIMARY_KEY = "key_id";
	
	private String indexKey;
	private MailMeta indexInfo; 
	private Path indexDir;

	
	
	
	public IndexWriter indexWriter;
	public static final Logger LOG=Logger.getLogger(IndexBuilder.class.getName());
	public IndexBuilder(Path indexDir) {
		this.indexDir = indexDir;
	}
	
	public IndexBuilder() {
		
	}
	
	
	public void prepare() throws CorruptIndexException, LockObtainFailedException, IOException {
        indexWriter = new IndexWriter(FSDirectory.open(indexDir.cat(LUCENE_INDEX_DIR).asFile()), 
					new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
        indexWriter.setMaxMergeDocs(9999999);
        indexWriter.setMergeFactor(300);
        indexWriter.setMaxBufferedDocs(300);
	}
	
	public void prepareByDir(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
		indexWriter = new IndexWriter(dir, new IKAnalyzer(false), true, IndexWriter.MaxFieldLength.LIMITED);
		
	}
	
	
	
	
	
	public void setIndexInfo(String id, MailMeta metainfo) {
        this.indexKey = id;
        this.indexInfo = metainfo;
    }
	
	public boolean index() {
	     
	    Document doc = convertToDocument();
	    if (doc == null) {
	        return false;
	    }
	
	    try {
			indexWriter.addDocument(doc);
		} catch (CorruptIndexException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
	
	    return true;
	}
	
	public void finish() throws IOException {
		indexWriter.optimize();
		indexWriter.close();
		indexWriter = null;
	}
	
	private Document convertToDocument() {
		 if(indexInfo.getContent()==null) {
	        	LOG.error("Mail's content is null!");
	        }
	        if(indexInfo.getSubject()==null) {
	        	LOG.error("Mail's subject is null!");
	        }
//LOG.info("email_id="+indexKey+" user_id="+indexInfo.getUsrid()+" content="+indexInfo.getContent()+" subject="+indexInfo.getSubject()+" from="+indexInfo.getFrom()+" to="+indexInfo.getTo());//Test Delete Index
        Document doc = new Document();

        doc.add(new Field(INDEX_PRIMARY_KEY, indexInfo.getUsrid()+"_" +indexKey,
                Field.Store.YES, Field.Index.NOT_ANALYZED));
     // global docID
        //doc.add(new Field(TAG_INDEX_DOC_ID, "" + indexKey,
        //       Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(MailConstants.CONTENT_FIELD, indexInfo.getContent(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(MailConstants.SUBJECT_FIELD, indexInfo.getSubject(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(MailConstants.FROM_FIELD, indexInfo.getFrom(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(MailConstants.TO_FIELD, indexInfo.getTo(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(MailConstants.TIME_FIELD, indexInfo.getTime()+"", Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.add(new Field(MailConstants.USR_FIELD, indexInfo.getUsrid()+"", Field.Store.NO, Field.Index.NOT_ANALYZED));
//        doc.add(new Field(MailConstants.FORLDER_FIELD, indexInfo.getFolder(), Field.Store.NO, Field.Index.NOT_ANALYZED));
        doc.add(new Field(MailConstants.ATTACH_FIELD, indexInfo.getAttachName(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(MailConstants.HASATT_FIELD, indexInfo.hasAttach()+"", Field.Store.NO, Field.Index.NOT_ANALYZED));
        return doc;
	}
	
	public boolean deleteIndexById(long emailId) throws CorruptIndexException, IOException {
		indexWriter.deleteDocuments(new Term(TAG_INDEX_DOC_ID, emailId+""));
		return false;
	}
	
	
	public static void main(String[] args) {
		/*Path p = new Path("d:\\test");
		IndexBuilder ib = new IndexBuilder(p);
		
		try {
			ib.prepare();
			ib.setIndexInfo(1L, new MailMeta("2", "2", "2", "2", 2L, 2L, 2L, "2", "2"));
			ib.index();
			ib.setIndexInfo(1L, new MailMeta("1", "1", "1", "1", 1L, 1L, 1L, "1", "1"));
			ib.index();
			ib.finish();
		} catch (CorruptIndexException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LockObtainFailedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		/*Map<String, List<Path>> a = new HashMap<String, List<Path>>();
		List<Path> aa = new ArrayList<Path>();
		aa.add(new Path("1"));
		aa.add(new Path("2"));
		a.put("1", aa);
		
		Map<String, List<Path>> b = new HashMap<String, List<Path>>();
		List<Path> bb = new ArrayList<Path>();
		bb.add(new Path("3"));
		bb.add(new Path("4"));
		b.put("1", bb);
		a = BuildMinorIndexTool.addMap(a, b);
		for (String ss : a.keySet()) {
			for (Path s : a.get(ss)) {
				System.out.println(ss+" " +s.getName());
			}
			
		}*/
		
		IndexSearcher searcher=null;
		try {
			searcher=new IndexSearcher(IndexReader.open(FSDirectory.open(new File("d://lucene_test/lucene_index"),null)));
		} catch (CorruptIndexException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
		Query cq=new TermQuery(new Term(TAG_INDEX_DOC_ID,"402"));
		TopDocs hits=null;
		try {
			 hits = searcher.search(cq, 100);
		} catch (IOException e) {
			e.printStackTrace();
		}   
		for (int i = 0; i < hits.scoreDocs.length; i++) {   
            ScoreDoc sdoc = hits.scoreDocs[i];   
            Document doc=null;
			try {
				doc = searcher.doc(sdoc.doc);
			} catch (CorruptIndexException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}   
            System.out.println("doc_id:"+doc.get("doc_id")+"   "+"content:"+doc.get("content")+"  "+"suject:"+doc.get("subject")+"   "+"time:"+doc.get("time")+"   "+"from:"+doc.get("from")+"   "+"to:"+doc.get("to"));               
        }      
	}
}
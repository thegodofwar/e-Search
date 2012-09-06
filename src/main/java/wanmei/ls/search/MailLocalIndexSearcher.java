package wanmei.ls.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StaleReaderException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.LockObtainFailedException;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.lucene.IKQueryParser;

import outpost.search.Hits;
import outpost.search.Hits.Hit;
import wanmei.is.data.MailMeta;
import wanmei.ls.LSUtils;
import wanmei.ls.data.MailIndexReader;
import wanmei.mina.handler.LocalIndexSearcher;
import wanmei.utils.MailConstants;
import wanmei.utils.XMLUtil;
import wanmei.utils.data.Path;
import wanmei.utils.index.IndexBuilder;

public class MailLocalIndexSearcher  extends LocalIndexSearcher {
	public static final Logger LOG=Logger.getLogger(MailLocalIndexSearcher.class.getName());
    
	public static int time=-1;
	public static int tag=-1;
	public static ConcurrentHashMap<String,List<String>> beforeCache=new ConcurrentHashMap<String,List<String>>();
	public static ConcurrentHashMap<String,List<String>> middleCache=new ConcurrentHashMap<String,List<String>>();
	public static ConcurrentHashMap<String,List<String>> afterCache=new ConcurrentHashMap<String,List<String>>();
	protected  MultiSearcher[] searchers;
	protected  MailIndexReader[] majorReaders;
	protected  MailIndexReader[] minorReaders;
	protected  MailIndexReader[] realTimeReaders;
	
	protected Analyzer analyzer = new IKAnalyzer(false);
	
	/*protected QueryParser contentParser = new QueryParser(Version.LUCENE_30,
			MailConstants.CONTENT_FIELD, analyzer);
	protected QueryParser subjectParser = new QueryParser(Version.LUCENE_30,
			MailConstants.SUBJECT_FIELD, analyzer);
	protected QueryParser fromParser = new QueryParser(Version.LUCENE_30,
			MailConstants.FROM_FIELD, analyzer);
	protected QueryParser toParser = new QueryParser(Version.LUCENE_30,
			MailConstants.TO_FIELD, analyzer);
	protected QueryParser attachParser = new QueryParser(Version.LUCENE_30,
			MailConstants.ATTACH_FIELD, analyzer);
	
	protected QueryParser[] parsers = new QueryParser[]{
		contentParser, fromParser, toParser,subjectParser,attachParser	
	};
	*/
	
	protected String[] searchFileds = new String[] {
		MailConstants.CONTENT_FIELD, MailConstants.FROM_FIELD, MailConstants.TO_FIELD,MailConstants.SUBJECT_FIELD,MailConstants.ATTACH_FIELD
	};
	protected  int partNo;
	protected  int slice;
	
	public MailLocalIndexSearcher(int slice, int partNo, int lsNum){
		this.slice = slice;
		this.partNo = partNo;
		this.lsNum = lsNum;
	}
	
	public void open(MailIndexReader[] majors, MailIndexReader[] minors, MailIndexReader[] realtimes) throws IOException {
		searchers = new MultiSearcher[partNo];
		majorReaders = majors;
		minorReaders = minors;
		realTimeReaders = realtimes; 
		for (int i=0; i<partNo; i++) {
			IndexSearcher majorSearcher = new IndexSearcher(majorReaders[i].getReader());
			
			if (realtimes != null && realtimes.length > 0) {
				if (minors != null) {
					IndexSearcher[] realTimeSearchers = new IndexSearcher[realTimeReaders.length+2];
					int realCount = realTimeReaders.length;
					for (int j=0; j<realCount; j++) {
						realTimeSearchers[j] = new IndexSearcher(realTimeReaders[j].getReader());
					}
					IndexSearcher minorSearcher = new IndexSearcher(minorReaders[i].getReader());
					realTimeSearchers[realCount] = minorSearcher;
					realTimeSearchers[realCount+1] = majorSearcher;
					
					searchers[i] = new MultiSearcher(realTimeSearchers);
				} else {
					IndexSearcher[] realTimeSearchers = new IndexSearcher[realTimeReaders.length+1];
					int realCount = realTimeReaders.length;
					for (int j=0; j<realCount; j++) {
						realTimeSearchers[j] = new IndexSearcher(realTimeReaders[j].getReader());
					}
					realTimeSearchers[realCount] = majorSearcher;
					
					searchers[i] = new MultiSearcher(realTimeSearchers);
				}
			} else {
				if (minors != null) {
					IndexSearcher minorSearcher = new IndexSearcher(minorReaders[i].getReader());
					searchers[i] = new MultiSearcher(majorSearcher, minorSearcher);
				} else {
					searchers[i] = new MultiSearcher(majorSearcher);
				}
			}
			
			/*int length = 1;
			if (minorReaders != null) {
				length += (minorReaders.length/partNo);
				
			}
			
			IndexSearcher[] newSearchers = new IndexSearcher[length];
			newSearchers[length-1] = majorSearcher;
			
			if (minorReaders != null && minorReaders.length > 0) {
				if ((minorReaders.length % partNo) != 0) {
					LOG.info("wrong partNo");
					return;
				} else {
					
					for (int j=0; j<minorReaders.length; j++) {
						if ((j % partNo)==i ) {
							newSearchers[j/partNo] = new IndexSearcher(minorReaders[j].getReader());
						}
					}
				}
			}*/
			
//			searchers[i] = new MultiSearcher(majorSearcher, minorSearcher);
		}
	}
	
	int lsNum;
	
	
	public int getLsNum() {
		return lsNum;
	}

	public void setLsNum(int lsNum) {
		this.lsNum = lsNum;
	}
	
    @Override
    public int delete(Map<String,String[]> params) {
    	String usrid = params.get(wanmei.utils.MailConstants.USR_FIELD)[0];
    	String delete_docIds[] = params.get(wanmei.utils.MailConstants.DELETE_DOCIDS);
    	
    	if(tag==0) {
    		if(beforeCache.containsKey(usrid)) {
       		 List<String> mSkeys=beforeCache.get(usrid);
       		 if(mSkeys==null) {
       			  List<String> newMSkeys=new ArrayList<String>();
       			  newMSkeys.addAll(Arrays.asList(delete_docIds));
       			  beforeCache.put(usrid, newMSkeys);
       		 } else {
       			  beforeCache.get(usrid).addAll(Arrays.asList(delete_docIds));
       		 }
         	} else {
       		  List<String> newMSkeys=new ArrayList<String>();
   			  newMSkeys.addAll(Arrays.asList(delete_docIds));
   			  beforeCache.put(usrid, newMSkeys);
         	}
    	  } else if(tag==1) {
    		  if(middleCache.containsKey(usrid)) {
           		 List<String> mSkeys=middleCache.get(usrid);
           		 if(mSkeys==null) {
           			  List<String> newMSkeys=new ArrayList<String>();
           			  newMSkeys.addAll(Arrays.asList(delete_docIds));
           			  middleCache.put(usrid, newMSkeys);
           		 } else {
           			  middleCache.get(usrid).addAll(Arrays.asList(delete_docIds));
           		 }
             	} else {
           		  List<String> newMSkeys=new ArrayList<String>();
       			  newMSkeys.addAll(Arrays.asList(delete_docIds));
       			  middleCache.put(usrid, newMSkeys);
             	}
    	  } else if(tag==2) {
    		  if(afterCache.containsKey(usrid)) {
          		 List<String> mSkeys=afterCache.get(usrid);
          		 if(mSkeys==null) {
          			  List<String> newMSkeys=new ArrayList<String>();
          			  newMSkeys.addAll(Arrays.asList(delete_docIds));
          			  afterCache.put(usrid, newMSkeys);
          		 } else {
          			  afterCache.get(usrid).addAll(Arrays.asList(delete_docIds));
          		 }
            	} else {
          		  List<String> newMSkeys=new ArrayList<String>();
      			  newMSkeys.addAll(Arrays.asList(delete_docIds));
      			  afterCache.put(usrid, newMSkeys);
            	}
    	  }
    	
        int realCount=0;
        if(realTimeReaders!=null) {
        IndexReader[] nowDeleteReaders=new IndexReader[realTimeReaders.length];
        for(int ii=0;ii<nowDeleteReaders.length;ii++) {
        	try {
				nowDeleteReaders[ii]=IndexReader.open(realTimeReaders[ii].getDirectory(), false);
			} catch (CorruptIndexException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
        }
    	for(IndexReader reader:nowDeleteReaders) {
    	  for(String doc_Id:delete_docIds) {
    		  try {
    			 realCount+=reader.deleteDocuments(new Term(IndexBuilder.INDEX_PRIMARY_KEY,usrid+"_"+doc_Id));
			} catch (StaleReaderException e) {
				LOG.error("",e);
			} catch (CorruptIndexException e) {
				LOG.error("",e);
			} catch (LockObtainFailedException e) {
				LOG.error("",e);
			} catch (IOException e) {
				LOG.error("",e);
			}
    	   }
    	}
    	for(IndexReader reader:nowDeleteReaders) {
    		try {
				reader.close();
			} catch (IOException e) {
				LOG.error("",e);
			}
    	}
       }
    	LOG.info("Delete RealTime Result: realCount="+realCount);
    	
    	//do logging
    	StringBuffer mskeys=new StringBuffer();
    	for(int i=0;i<delete_docIds.length;i++) {
		   	if(i<delete_docIds.length-1) {
		   		mskeys.append(delete_docIds[i]+",");
		   	} else if(i==delete_docIds.length-1){
		   		mskeys.append(delete_docIds[i]);
		   	}
		}
    	LOG.info("usrid="+usrid+" "+"delete_mskeys="+mskeys.toString()+" !slice="+slice);
    	LOG.info("LS Get The DS' Delete Request!...^_^...!slice="+slice);
    	return  1;
    }
    
	@Override
	public Hits search(String query, int start, int len, Map<String, String[]> params) {
		/**
		 * delete index test
		 */
		/*LOG.info("tag="+tag+" time="+time);
		for(Map.Entry<String, List<String>> entry:beforeCache.entrySet()) {
			LOG.info("beforeCache :"+entry.getKey()+" : "+Arrays.toString(entry.getValue().toArray(new String[]{})));
		}
		for(Map.Entry<String, List<String>> entry:middleCache.entrySet()) {
			LOG.info("middleCache :"+entry.getKey()+" : "+Arrays.toString(entry.getValue().toArray(new String[]{})));
		}
		for(Map.Entry<String, List<String>> entry:afterCache.entrySet()) {
			LOG.info("afterCache :"+entry.getKey()+" : "+Arrays.toString(entry.getValue().toArray(new String[]{})));
		}*/
		
		Hits hits = new Hits();
		

		String usrid = params.get(wanmei.utils.MailConstants.USR_FIELD)[0];
				
		
		LOG.info("[INFO] action=search q="+query+" start="+start+" length="+len
				+" usrid="+usrid);
		
		//不判断ls了，直接所有ls都进行搜索
		/*if (!LSUtils.checkLS(usrid, slice, lsNum)) {
			return hits;
		}*/
		Query q = parseQuery(params);
//		TopScoreDocCollector collector = TopScoreDocCollector.create(len, false);

		long starttime = System.currentTimeMillis();
		
		int partNo = Integer.parseInt(XMLUtil.readValueStrByKey("partNo"));
		int threadNo=Integer.parseInt(XMLUtil.readValueStrByKey("index_max_thread_num"));
		try {
			int part = ((int)(Math.abs(Long.parseLong(usrid)%threadNo)))%partNo;
//			LOG.info("part:"+part+" partNo:"+partNo + " usrid:"+usrid);
			
			MultiSearcher searcher = searchers[part];
//			LOG.info("totalseacher:"+searcher.getSearchables().length);;
			
			String[] sortParams = params.get(MailConstants.SORTTYPE);
			Sort sort = parseSort(sortParams);
			
//			LOG.info("sort:"+sort.toString());
			TopFieldDocs docs=null;
			try {
			docs = searcher.search(q, null, len, sort);
			} catch(Exception e) {
			LOG.error("The search keywords are illlegal!",e);
			}
//			searcher.search(q, collector);
			
			LOG.info("[INFO] action=search_end q="+q+" sort="+sort+" total="
					+docs.totalHits +" time_used="+(System.currentTimeMillis()- starttime) + " ms");
//			System.out.println("["+q+"] total "+docs.totalHits+" used "+(System.currentTimeMillis()- starttime) + " ms");//			TopDocs docs = collector.topDocs();

			
//			int totalHits = collector.getTotalHits();
			int totalHits = docs.totalHits;
            if (totalHits < start) {
                // 没有start开始的doc
                LOG.info(String.format("Will return empty Hits %d(totalHits) < %d(start)", totalHits, start));
                return hits;
            }

			ScoreDoc[] scoreDocs = docs.scoreDocs;
			LOG.info(String.format("Total hits: %d; top hits: %d", totalHits, scoreDocs.length));
			Set<Long> tempSet=new HashSet<Long>();
			for (ScoreDoc scoreDoc : scoreDocs) {
				Document d = searcher.doc(scoreDoc.doc);
				//String docid = d.getField(IndexBuilder.TAG_INDEX_DOC_ID).stringValue();
				String docid = d.getField(IndexBuilder.INDEX_PRIMARY_KEY).stringValue().split("[_]")[1];
				if(beforeCache.containsKey(usrid)) {
					 if(beforeCache.get(usrid).contains(docid)) {
						 continue;
					 }
				}
				if(middleCache.containsKey(usrid)) {
					 if(middleCache.get(usrid).contains(docid)) {
						 continue;
					 }
				}
				if(afterCache.containsKey(usrid)) {
					 if(afterCache.get(usrid).contains(docid)) {
						 continue;
					 }
				}
			  if(!tempSet.contains(Long.parseLong(docid))) {
				Hit hit = new Hit();
//				LOG.info("docid:"+docid+" ["+d.getField(IndexBuilder.TAG_INDEX_DOC_ID).stringValue());
				hit.setDocID(Long.parseLong(docid));
				hit.setScore(scoreDoc.score);
				hits.add(hit);
				tempSet.add(Long.parseLong(docid));
			  }
			}
			tempSet.clear();
			tempSet=null;
			hits.setTotal(totalHits);
		} catch (NumberFormatException e) {
			LOG.error("",e);
		} catch (CorruptIndexException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		} catch (Exception e) {
			LOG.error("",e);
		}	

		return hits;
    }
    
	private Sort parseSort(String[] sortParams) {
    	
    	if (sortParams == null || sortParams.length < 2) {
    		return new Sort(new SortField(MailConstants.TIME_FIELD, SortField.STRING, true));
    	} 
//    	LOG.info("start parse sort :"+sortParams[0]+" "+sortParams[1]);
    	String sortType = sortParams[0];
    	String desc = sortParams[1];
    	boolean reverse = true;
    	if (desc.equals("0")) {
    		reverse = false;
    	}
    	
    	String sf = MailConstants.TIME_FIELD;
    	if (sortType.equals("1")) {
    		sf = MailConstants.SUBJECT_FIELD;
    	} else if (sortType.equals("2")) {
    		sf = MailConstants.FROM_FIELD;
    	}
    	
    	return new Sort(new SortField(sf, SortField.STRING, reverse)); 
		
	}

	private Query parseQuery(Map<String, String[]> params) {
		
    	String usrid = params.get(wanmei.utils.MailConstants.USR_FIELD)[0];
    	String starttime = params.get(MailConstants.STARTTIME_FILED)[0];
    	String endtime = params.get(MailConstants.ENDTIME_FILED)[0];
//    	String folders = params.get(MailConstants.FORLDER_FIELD)[0];
    	String operator = params.get(MailConstants.OPERATOR)[0];
    	String hasAtt = params.get(MailConstants.HASATT_FIELD)[0];
    	
    	String[] queries = params.get(MailConstants.SEARCHFIELD_FIELD);
    	BooleanQuery q = new BooleanQuery();
    	
    	TermQuery uq = new TermQuery(new Term(MailConstants.USR_FIELD, usrid));
    	q.add(uq, Occur.MUST);
    	
    	BooleanQuery tq = new BooleanQuery();
    	
    	Occur occur = Occur.SHOULD;
    	if (operator.equals(MailConstants.AND)){
    		occur = Occur.MUST;
    	}
    	try {
    		boolean hasQuery = false;
			for (int i=0; i<searchFileds.length; i++) {
				if (queries[i] != null && queries[i].length()>0 ) {
					hasQuery = true;
//					System.out.println("parser"+i+" q:"+queries[i]+" occur:"+operator);
					String filed = searchFileds[i];
					Query cq = IKQueryParser.parse(filed, queries[i]);
//					Query cq = parsers[i].parse(queries[i]);
					tq.add(cq, occur);
				} 
				
			}
			
			if (hasQuery) { 
				q.add(tq, Occur.MUST);
			}
			
		
    		
		} catch (IOException e) {
			LOG.error("",e);
		}
		
		if (hasAtt != null && hasAtt.length() > 0 && !hasAtt.equals("0")) {
			TermQuery hasAttQuery = new TermQuery(new Term(MailConstants.HASATT_FIELD, hasAtt));
			q.add(hasAttQuery, Occur.MUST);
		}
		
		/*if (folders != null && folders.length() > 0) {
			if (folders.indexOf(MailConstants.FOLDER_ALL)<0) {
				String[] folder = folders.split(",");
				BooleanQuery foQuery = new BooleanQuery();
				for (String fo : folder) {
					if (fo.equals(MailConstants.FOLDER_INBOX)) {
						TermQuery termq = new TermQuery(new Term(MailConstants.FORLDER_FIELD, MailConstants.FOLDER_INBOX)); 
						foQuery.add(termq, Occur.SHOULD);
					} else if (fo.equals(MailConstants.FOLDER_SENDBOX)) {
						TermQuery termq = new TermQuery(new Term(MailConstants.FORLDER_FIELD, MailConstants.FOLDER_SENDBOX)); 
						foQuery.add(termq, Occur.SHOULD);
					}
				}
				q.add(foQuery, Occur.MUST);
			}
		}*/
		
		
		if (starttime.length() > 0 && !starttime.equals("0")) {
			if (endtime.length() <= 0) {
				endtime = System.currentTimeMillis()+"";
			}
			Query timeRangeQuery = new TermRangeQuery(MailConstants.TIME_FIELD, starttime, endtime, true, true);
			q.add(timeRangeQuery, Occur.MUST);
		} 

		
		
		return q;
    	
    	
	}
    
    
    
    public void closeAll() throws IOException {
    	if (realTimeReaders != null) {
    		for (MailIndexReader reader : realTimeReaders) {
                reader.closeWithOutDir();
            }
            realTimeReaders = null;
    	}
    	
        if (majorReaders != null) {
            for (MailIndexReader reader : majorReaders) {
                reader.close();
            }
            majorReaders = null;
        }
        
        if (minorReaders != null) {
            for (MailIndexReader reader : minorReaders) {
                reader.close();
            }
            minorReaders = null;
        }
        
        if (searchers != null) {
            for (MultiSearcher searcher : searchers) {
                searcher.close();
            }
            searchers = null;
        }
    }
    
    public void close() throws IOException {
    	
        if (majorReaders != null) {
            for (MailIndexReader reader : majorReaders) {
                reader.close();
            }
            majorReaders = null;
        }
        
        if (minorReaders != null) {
            for (MailIndexReader reader : minorReaders) {
                reader.close();
            }
            minorReaders = null;
        }
        
        if (searchers != null) {
            for (MultiSearcher searcher : searchers) {
                searcher.close();
            }
            searchers = null;
        }
    }
    
    public  MultiSearcher[] getSearchers() {
    	return searchers;
    }
    
	public  MailIndexReader[] getMajorReaders() {
		return majorReaders;
	}

	
	public  MailIndexReader[] getRealTimeReaders() {
		return realTimeReaders;
	}

	public  MailIndexReader[] getMinorReaders() {
		return minorReaders;
	}

    public static void main(String[] args) {
    	Path p = new Path("d:\\test");
    	IndexBuilder builder = new IndexBuilder(p);
    	try {
			builder.prepare();
			builder.setIndexInfo("1", new MailMeta("what", "test", "0", "0", 11L, System.currentTimeMillis(), 0, "1", "",""));
			builder.index();
			builder.finish();
		} catch (CorruptIndexException e) {
			LOG.error("",e);
		} catch (LockObtainFailedException e) {
			LOG.error("",e);
		} catch (IOException e) {
			LOG.error("",e);
		}
		
		MailLocalIndexSearcher searcher = new MailLocalIndexSearcher(0, 1, 1);
		MailIndexReader[] majors = new MailIndexReader[1];
		try {
			majors[0] = new MailIndexReader();
			majors[0].open(p.cat(IndexBuilder.LUCENE_INDEX_DIR).getAbsolutePath(), false);
			searcher.open(majors, null, null);
			Map<String, String[]> map = new HashMap<String, String[]>();
			map.put("usrid", new String[]{"0"});
			map.put("stime", new String[]{""});
			map.put("etime", new String[]{""});
			map.put("op", new String[]{""});
			map.put("hasatt", new String[]{"0"});
			map.put(MailConstants.SEARCHFIELD_FIELD, new String[]{"test","","",""});
	    	
			Hits hits = searcher.search("what", 0, 10, map);
			LOG.info(hits.getTotal());
		} catch (IOException e) {
			LOG.error("",e);
		}
    }
		
}
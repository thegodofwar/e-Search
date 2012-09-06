package pf.ds;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

import outpost.search.Hits;
import outpost.service.ServiceError;
import pf.ds.data.SearchResult;
import pf.is.data.MailMeta;
import pf.mina.MinaServiceMaster;
import pf.mina.IndexSearchHandlerAdapters.IndexSearcher;
import pf.utils.MailConstants;
import pf.utils.XMLUtil;

public class MailDistributedSearcher {
	public static final Logger LOG=Logger.getLogger(MailDistributedSearcher.class.getName());
	private MinaServiceMaster master;
    private IndexSearcher searcher;
    private static MailDistributedSearcher instance = null; 
     
    
    public void init(){
    	
    	int dsPort = Integer.parseInt(XMLUtil.readDSConfig("ls_send_ds_result"));
    	try {
			master = new MinaServiceMaster(dsPort, 16, 150);
		} catch (IOException e) {
               LOG.error("",e);
		}
    	searcher = new IndexSearcher();
        searcher.setMaster(master);
        searcher.setServiceType(MailConstants.INDEXSERVICE);
        
        instance = this;
    }
    
    public static MailDistributedSearcher getInstance() {
    	if (instance == null) {
    		MailDistributedSearcher searcher = new MailDistributedSearcher();
    		searcher.init();
    	} 
    	
    	return instance;
    }
    
    private MailDistributedSearcher () {
    	
    }
    
    public SearchResult search(String q, int start, int len, Map<String, String[]> params) {
    	SearchResult sr = new SearchResult();
        try {
        	ServiceError error = new ServiceError();
        	long ss = System.currentTimeMillis();
            Hits hits = searcher.search(q, start, len, params, MailConstants.DS_TTL, error, len);
            StringBuilder sb=new StringBuilder();
            int tempI=1;
            for(Map.Entry<String,String[]> entry:params.entrySet()) {
            	sb.append(entry.getKey()+"<--->"+Arrays.toString(entry.getValue()));
            	if(tempI<params.size()) {
            	 sb.append(";");
            	}
            	tempI++;
            }
            LOG.info("[INFO] action=ds_search q="+q+" start="+start+" length="+len
        			+" params="+sb.toString() +" time_used="+(System.currentTimeMillis()-ss)+"ms");
            IKSegmentation segmentation = new IKSegmentation(new StringReader(q), true);
            Lexeme lm =  null;
            List<String> queryList = new ArrayList<String>();
            while ((lm = segmentation.next()) != null){
            	queryList.add(lm.getLexemeText());
            }
            sr.setQuery(queryList);
            
            if (hits != null) {
            	MailMeta[] mails = new MailMeta[hits.size()];
            	
            	//TODO: mail = genResultFromMSS(longids);
            	for (int i=0; i<hits.size();i++) {
            		long id = hits.get(i).getDocID();
            		mails[i] = new MailMeta("", "", "", "", id, 0, 1, "1","","");
            	}
            	sr.setMails(mails);
            }
        } catch (Exception e) {
        	LOG.error("",e);
		}
        
        return sr;
    }
    
    public int delete(Map<String,String[]> params) {
    	long start=System.currentTimeMillis();
    	int re_code=searcher.delete(params,MailConstants.DS_TTL);
    	LOG.info("Used time for deleting:"+(System.currentTimeMillis()-start)+"ms");
    	return re_code;
    }
}

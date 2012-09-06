package pf.web;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import odis.serialize.lib.MD5Writable;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

import pf.ds.MailDistributedSearcher;
import pf.ds.data.SearchResult;
import pf.is.data.MailMeta;
import pf.utils.MailConstants;
import pf.utils.ParamUtil;


public class MailSearchController implements Controller {
	public static final Logger LOG=Logger.getLogger(MailSearchController.class.getName());

	private static final int DEFAULTSTART = 0;
	private static final int DEFAULTLEN = 20000;
	
	
	protected MailDistributedSearcher dsSearcher;
	protected String errorPath;
	protected String viewPath;

	public void init(){
		
	}
	
	
	@Override
	public ModelAndView handleRequest(HttpServletRequest req,
			HttpServletResponse res) throws Exception {
		HashMap<String, Object> model = new HashMap<String, Object>();
		
        if (dsSearcher == null) {
            LOG.info("[ERROR] dsSearcher is null");
            model.put("type", "exception");
            return new ModelAndView(errorPath, "model", model);
        } 
        
        
        //pars req param
        Map<String, String[]> params = new HashMap<String, String[]>();
        
        //usr
        String usridStr = ParamUtil.getString(req, MailConstants.USR_FIELD, "0");
        
        String query = req.getParameter(MailConstants.QUERY_FILED);
        LOG.info("[INFO] action=web_request usrid="+usridStr+" q="+query);
        String usrid = usridStr;
        
        //sorttype
        //String sort = ParamUtil.getString(req, MailConstants.SORTTYPE, "");
        
        //time range
        String starttime = ParamUtil.getString(req, MailConstants.STARTTIME_FILED, "");
        String endtime = ParamUtil.getString(req, MailConstants.ENDTIME_FILED, "");
        
        //folder
//        String folders = ParamUtil.getString(req, MailConstants.FORLDER_FIELD, MailConstants.FOLDER_ALL+"");
        
        String multiQueryType = MailConstants.OR;
        if (query == null) {
        	multiQueryType = MailConstants.AND;
        	query = "";
        }
        String attachStr=null;
        if(query == null)
        	attachStr = "";
        else
        	attachStr = query;
        
        
        String contentQuery = ParamUtil.getString(req, MailConstants.CONTENT_QUERY, query).toLowerCase().trim();
        String fromQuery = ParamUtil.getString(req, MailConstants.From_QUERY, query).toLowerCase().trim();
        String toQuery = ParamUtil.getString(req, MailConstants.TO_QUERY, query).toLowerCase().trim();
        String subjectQuery = ParamUtil.getString(req, MailConstants.SUBJECT_QUERY, query).toLowerCase().trim();
        String doAttachQuery = ParamUtil.getString(req, MailConstants.ATTACH_NAMES, attachStr).toLowerCase().trim();
        String attachQuery = ParamUtil.getString(req, MailConstants.ATTACH_QUERY, "0").toLowerCase().trim();
        String sortType = ParamUtil.getString(req, MailConstants.SORTTYPE, "0");
        String desc = ParamUtil.getString(req, "desc", "1");
        
        
        
        
        params.put(MailConstants.USR_FIELD, new String[]{usrid});
        //params.put(MailConstants.SORTTYPE, new String[]{sort});
        params.put(MailConstants.STARTTIME_FILED, new String[]{starttime});
        params.put(MailConstants.ENDTIME_FILED, new String[]{endtime});
        
//        params.put(MailConstants.FORLDER_FIELD, new String[]{folders});
        
        params.put(MailConstants.OPERATOR, new String[]{multiQueryType});
        params.put(MailConstants.SEARCHFIELD_FIELD, new String[]{contentQuery, fromQuery, toQuery, subjectQuery,doAttachQuery});
        params.put(MailConstants.HASATT_FIELD, new String[]{attachQuery});
        params.put(MailConstants.SORTTYPE, new String[] {sortType, desc}); 
        
        
        int start = ParamUtil.getInt(req, MailConstants.START_FILED, DEFAULTSTART);
        int len = ParamUtil.getInt(req, MailConstants.LENGTH_FILED, DEFAULTLEN);
        
        
//        System.out.println("query:"+query);
        LOG.info("DS get search request:" + query);
        SearchResult sr = dsSearcher.search(contentQuery, start, len, params);
        StringBuffer docIds = new StringBuffer();
        for(MailMeta mm : sr.getMails()){
        	docIds.append(mm.getId());
        	docIds.append(",");
        }
        LOG.info("DS get search result: " + sr.getMails().length + " ids:" + docIds.toString());
        putSrInModel(model,sr);
        
        
		return new ModelAndView(viewPath, model);
	}


	private void putSrInModel(HashMap<String, Object> model, SearchResult sr) {
		model.put("qs", sr.getQuery());
		model.put("mails", sr.getMails());
	}


	public MailDistributedSearcher getDsSearcher() {
		return dsSearcher;
	}


	public void setDsSearcher(MailDistributedSearcher dsSearcher) {
		this.dsSearcher = dsSearcher;
	}


	public String getErrorPath() {
		return errorPath;
	}


	public void setErrorPath(String errorPath) {
		this.errorPath = errorPath;
	}


	public String getViewPath() {
		return viewPath;
	}


	public void setViewPath(String viewPath) {
		this.viewPath = viewPath;
	}
}

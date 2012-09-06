package wanmei.web;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import odis.serialize.lib.MD5Writable;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

import wanmei.ds.MailDistributedSearcher;
import wanmei.utils.MailConstants;
import wanmei.utils.ParamUtil;

public class DeleteIndexController implements Controller {
	public static final Logger LOG=Logger.getLogger(DeleteIndexController.class.getName());
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
            LOG.error("[ERROR] dsSearcher is null");
            model.put("type", "exception");
            return new ModelAndView(errorPath, "model", model);
        } 
		
		//the params of deleting index
		Map<String, String[]> params = new HashMap<String, String[]>();
		//userIdStr
		String usridStr = ParamUtil.getString(req, MailConstants.USR_FIELD, "0");
		String usrid = usridStr;
		//delete index by the parameter of mskey
		String delete_mskeys[] = req.getParameterValues(MailConstants.DELETE_DOCID);
		if(delete_mskeys==null||delete_mskeys.length==0) {
			LOG.error("Could not execute deleting index because of no delelte_mskey reference");
			model.put("type", "exception");
			return new ModelAndView(errorPath,"model",model);
		}
		//put reference into map params
		params.put(MailConstants.USR_FIELD, new String[]{usrid});
		params.put(MailConstants.DELETE_DOCIDS, delete_mskeys);
		//do deleting index
		StringBuffer mskeys=new StringBuffer();
		for(int i=0;i<delete_mskeys.length;i++) {
		   	if(i<delete_mskeys.length-1) {
		   		mskeys.append(delete_mskeys[i]+",");
		   	} else if(i==delete_mskeys.length-1){
		   		mskeys.append(delete_mskeys[i]);
		   	}
		}
		LOG.info("DS get delete request: "+usridStr+" "+mskeys.toString());
		int delResult=dsSearcher.delete(params);
		if(delResult==1) {
		  	LOG.info("delete index according to msskey successfully!");
		} else if(delResult==0) {
			LOG.error("delete index according to msskey failure!");
		}
		model.put("dr", delResult);
		return new ModelAndView(viewPath,model);
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

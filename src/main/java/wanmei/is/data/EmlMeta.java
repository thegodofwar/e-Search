package wanmei.is.data;

import java.util.ArrayList;
import java.util.List;

public class EmlMeta {
	private String subject;
	private String from;
	private String to;
	private String txtBody;
	private String htmlBody;
	private List<String> attaches = new ArrayList<String>();
	
	public EmlMeta() {
		this.subject = "";
		this.from = "";
		this.to = "";
		this.txtBody = "";
		this.htmlBody = "";
	}
	
	public EmlMeta(String subject, String from, String to, String txtBody, String htmlBody){
		this.subject = subject;
		this.from = from;
		this.to = to;
		this.txtBody = txtBody;
		this.htmlBody = htmlBody;
	}

	public String getSubject() {
		if (subject == null) {
			return "";
		}
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getTxtBody() {
		return txtBody;
	}

	public void setTxtBody(String txtBody) {
		this.txtBody = txtBody;
	}

	public String getHtmlBody() {
		return htmlBody;
	}

	public void setHtmlBody(String htmlBody) {
		this.htmlBody = htmlBody;
	}
	
	public void addAttach(String attachName) {
		this.attaches.add(attachName);
	}
	
	public String getAttaches() {
		StringBuffer sb = new StringBuffer();
		for (String att : attaches) {
			sb.append(att).append(",");
		}
		return sb.toString();
	}
	
	
	

}

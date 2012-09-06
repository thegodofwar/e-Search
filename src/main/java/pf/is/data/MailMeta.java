package pf.is.data;

public class MailMeta {
	public long id;
	public String subject;
	public String content;
	public String from;
	public String to;
	public long time;
	public long usrid;
	public String folder;
	public String attachName;
	public String docid;
	
	public MailMeta(String subject, String content, String from, String to, long id, long time, long usr, String folder, String attachName, String docid) {
		this.id = id;
		if(subject==null) {
			subject="";
		}
		this.subject = subject.toLowerCase();
		if(content==null) {
			content="";
		}
		this.content = content.toLowerCase();
		if(from==null) {
			from="";
		}
		this.from = from.toLowerCase();
		if(to==null) {
			to="";
		}
		this.to = to.toLowerCase();
		this.time = time;
		this.usrid = usr;
		if(folder==null) {
			folder="";
		}
		this.folder = folder.toLowerCase();
		if(attachName==null) {
			attachName="";
		}
		this.attachName = attachName.toLowerCase();
		this.docid = docid;
	}
	
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
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

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getUsrid() {
		return usrid;
	}

	public void setUsrid(long usrid) {
		this.usrid = usrid;
	}

	public String getFolder() {
		return folder;
	}

	public void setFolder(String folder) {
		this.folder = folder;
	}

	public String getAttachName() {
		return attachName;
	}
	
	public int hasAttach() {
		if (attachName != null && attachName.length() > 0) {
			return 1;
		} else {
			return 2;
		}
	}

	public void setAttachName(String attachName) {
		this.attachName = attachName;
	}

	public String getDocid() {
		return docid;
	}

	public void setDocid(String docid) {
		this.docid = docid;
	}
	

	
}

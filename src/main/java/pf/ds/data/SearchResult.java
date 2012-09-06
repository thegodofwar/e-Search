package pf.ds.data;

import java.util.List;

import pf.is.data.MailMeta;

public class SearchResult {
	public long id;
	public List<String> query;
	public String sort;
	public int totalNumber = 0;
	
	public MailMeta[] mails;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public List<String> getQuery() {
		return query;
	}

	public void setQuery(List<String> query) {
		this.query = query;
	}

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public MailMeta[] getMails() {
		return mails;
	}

	public void setMails(MailMeta[] mails) {
		this.mails = mails;
	}

	public int getTotalNumber() {
		return totalNumber;
	}

	public void setTotalNumber(int totalNumber) {
		this.totalNumber = totalNumber;
	}
	
	


}

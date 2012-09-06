package pf.utils.wordsegment;


public class TermInfo {
	private String term;
	private int start;
	
	public TermInfo(String term, int start) {
		this.term = term;
		this.start = start;
	}
	
	public String getTerm() {
		return term;
	}
	public void setTerm(String term) {
		this.term = term;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
}

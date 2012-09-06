package pf.utils.wordsegment;


public interface Segmentor {

	public TermInfo[] doIndexSegment(String input);
	public TermInfo[] doQuerySegment(String input);
	public void close();
}

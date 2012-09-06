package pf.utils.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pf.utils.PathUtil;


/**
 * View of segments under one directory. Tools to create, find, mark/unmark 
 * segment are included.
 * 
 * Sample: get the last segment and mark it with tag "tag/filtered".
 * <code>
 * Segments segments = new Segments(fs, "segments");
 * segments.mark(segments.getLastSegment(), new String[]{"tag/filtered"});
 * </code>
 * 
 * @author changwei
 *
 */
public class Segments {

    private static Pattern pattern = Pattern.compile("(\\d+)");
    
    private Path path;
    
    /**
     * Create segments at given path.
     * @param fs
     * @param path
     */
    public Segments(Path path) {
        this.path = path;
    }
    
    /**
     * Return if the segment path exists.
     * @return
     * @throws IOException
     */
    public boolean exists() throws IOException {
        return path.asFile().exists();
    }
    
    /**
     * Return the list of segment ids in numeric ascending order.
     * @return
     * @throws IOException
     */
    public List<Integer> getSegments() throws IOException {
        File [] childs = path.asFile().listFiles();
        if (childs == null || childs.length == 0) {
            return new ArrayList<Integer>();
        }
        List<Integer> list = new ArrayList<Integer>();
        for (File file : childs) {
            Matcher matcher = pattern.matcher(file.getName());
            if (!matcher.matches()) continue;
            list.add(Integer.parseInt(matcher.group(1)));
        }
        Collections.sort(list);
        return list;
    }
    
    /**
     * @return the segment with max id. -1 is returned if the segment dir
     * is empty.
     * @throws IOException
     */
    public int getLastSegment() throws IOException {
        List<Integer> segs = getSegments();
//        return segs.get(segs.size() - 1);
        return (segs.size()==0) ? -1 : segs.get(segs.size()-1);
    }
    
    /**
     * Return path for given segment.
     * @param seg
     * @return
     */
    public Path getSegmentPath(int seg) {
        return path.cat(String.valueOf(seg));
    }
    
    /**
     * Check if the given segment contains tags in marked.
     * @param seg
     * @param marked
     * @return
     * @throws IOException
     */
    public boolean match(int seg, String ... marked) throws IOException {
        return match(seg, marked, EmptyInstance.STRINGS);
    }
    
    /**
     * Check if the given segment contains tags in marked and not contains tags 
     * in notMarked.
     * 
     * @param seg  the index of the segment to be checked
     * @param marked  the marks that should exist. The parameter could be null
     *                which has the same effects as an empty String array.
     * @param notMarked  the marks that should NOT exist. The parameter could be 
     *                   null which has the same effects as an empty String 
     *                   array.
     * @return  true if checking passed, false otherwise
     * @throws IOException  if an I/O error occurs.
     */
    public boolean match(int seg, String [] marked, String [] notMarked) 
            throws IOException {
        Path segmentPath = getSegmentPath(seg);
        if (marked != null) {
            for (String tag: marked) {
                Path tagPath = segmentPath.cat(tag);
                if (!tagPath.asFile().exists()) 
                    return false;
            } // for tag
        } // if
        if (notMarked != null) {
            for (String tag: notMarked) {
                Path tagPath = segmentPath.cat(tag);
                if (tagPath.asFile().exists()) 
                    return false;
            } // for tag
        } // if
        return true;
    }
    
    /**
     * Mark given tags in segment specified path.
     * @param segPath
     * @param marks
     * @throws IOException
     */
    public void mark(Path segmentPath, String ... marks) throws IOException {
        for (String tag : marks) {
            Path tagPath = segmentPath.cat(tag);
            if (!tagPath.asFile().exists()) {
                tagPath.asFile().mkdirs();
            }
        }
    }
    
    /**
     * Mark the given tags in segment.
     * @param seg
     * @param mark
     * @throws IOException
     */
    public void mark(int seg, String ... marks) throws IOException {
        Path segmentPath = getSegmentPath(seg);
        mark(segmentPath, marks);
    }
    
    /**
     * Mark several segments with given marks.
     * @param segs
     * @param marks
     * @throws IOException
     */
    public void mark(int [] segs, String ... marks) throws IOException {
        for (int seg : segs) mark(seg, marks);
    }
    
    /**
     * Unmark the tags in segment specified by path.
     * @param segmentPath
     * @param marks
     * @throws IOException
     */
    public void unmark(Path segmentPath, String ... marks) throws IOException {
        for (String tag : marks) {
            Path tagPath = segmentPath.cat(tag);
            if (tagPath.asFile().exists()) {
            	tagPath.asFile().delete();
            }
        }
    }
    
    /**
     * Unmark the given tags in segment.
     * @param seg
     * @param mark
     * @throws IOException
     */
    public void unmark(int seg, String ... marks) throws IOException {
        Path segmentPath = getSegmentPath(seg);
        unmark(segmentPath, marks);
    }
    
    /**
     * Unmark the given tags.
     * @param segs
     * @param marks
     * @throws IOException
     */
    public void unmark(int [] segs, String ... marks) throws IOException {
        for (int seg : segs) unmark(seg, marks);
    }
    
    /**
     * Create a new segment under segment path.
     * @return
     * @throws IOException
     */
    public int createNewSegment() throws IOException {
        List<Integer> segs = getSegments();
        int newSeg = segs.size() == 0 ? 0 : (segs.get(segs.size()-1) +1);
        PathUtil.mkdirs(path.cat(String.valueOf(newSeg)));
        return newSeg;
    }
    
    /**
     * Delete segment.
     * @param seg
     * @throws IOException
     */
    public void deleteSegment(int seg) throws IOException {
        
            Path segmentPath = getSegmentPath(seg);
            if (segmentPath.asFile().exists()) {
                segmentPath.asFile().delete();
            }
        
    }
    
    /**
     * Find the segments contains tags in marked and not contains tags in 
     * notMarked. Returned segments in numeric ascending order.
     * @param marked  the marks that should exist. The parameter could be null
     *                which has the same effects as an empty String array.
     * @param notMarked  the marks that should NOT exist. The parameter could be 
     *                   null which has the same effects as an empty String 
     *                   array.
     * @return
     */
    public List<Integer> findSegments(String [] marked, String [] notMarked) 
            throws IOException {
        List<Integer> segs = getSegments();
        List<Integer> selected = new ArrayList<Integer>();
        for (int seg : segs) {
            if (match(seg, marked, notMarked)) {
                selected.add(seg);
            }
        }
        return selected;
    }
}
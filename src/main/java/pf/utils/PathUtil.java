package pf.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import pf.utils.data.Path;


public class PathUtil {
	public static final Logger LOG=Logger.getLogger(PathUtil.class.getName());
	
    // by default use copy/delete instead of rename
    static boolean useCopyForRename = true;
	
    
    public static void mkdirs(Path path) {
    	path.asFile().mkdirs();
    }
    
    public static boolean exists(Path path) {
    	return path.asFile().exists();
    }
    
    public static boolean delete(Path path) throws IOException {
    	if (path.asFile().isFile()) {
            return path.asFile().delete();
        } else return fullyDelete(path.asFile());
    }
    
    public static  boolean fullyDelete(File dir) throws IOException {
        File contents[] = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (! contents[i].delete()) {
                        return false;
                    }
                } else {
                    if (! fullyDelete(contents[i])) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }
    
	public static void replace(Path target, Path newPath, 
            Path tmpPath, boolean keepTmp) throws IOException {
		if (!exists(newPath))
            throw new IOException("cannot find new file " + newPath);

        if (exists(tmpPath) && !delete(tmpPath))
            throw new IOException("cannot remove tmp file " + tmpPath
                    + " during replace");

        // create the parent directory for target
        if (!exists(target.getParentFile())) {
            mkdirs(target.getParentFile());
        }
        
        boolean restore = exists(target);

        if (exists(target) && !rename(target, tmpPath))
            throw new IOException("cannot rename old file " + target
                    + " to tmp file " + tmpPath);

        if (!rename(newPath, target))
            if (restore) {
                LOG.info("Cannot rename new file " + newPath  + " to file " 
                        + target +", restoring the old one from " + tmpPath);
                if (!rename(tmpPath, target))
                    throw new IOException("Cannot restore " + target + " from " + tmpPath);
            } else throw new IOException("Cannot rename new file " + newPath
                    + " to file " + target);

        if (!keepTmp)
            if (exists(tmpPath) && !delete(tmpPath))
                throw new IOException("cannot clean tmp file after replace finished");
    }
    
	public static boolean rename(Path src, Path dst) throws IOException {
		if (useCopyForRename) {
            if (dst.asFile().exists()) {
                return false;
            } else {
                if (!src.asFile().renameTo(dst.asFile())) {
                    copyContents(src, dst, true);
                    return fullyDelete(src.asFile());
                } else {
                    return true;
                }
            }
        } else return src.asFile().renameTo(dst.asFile());
    }
	
	
    public static final int DEFAULT_READ_BUFFER_SIZE = 64 * 1024;

    public static final int DEFAULT_WRITE_BUFFER_SIZE = 64 * 1024;
	/**
     * Copy a file's contents to a new location. Returns whether a target file
     * was overwritten
     */
    public static boolean copyContents(Path src, Path dst,
            boolean overwrite) throws IOException {
        if (exists(dst) && !overwrite) {
            return false;
        } //if

        Path dstParent = dst.getParentFile();
        if ((dstParent != null) && (!exists(dstParent))) {
            mkdirs(dstParent);
        } // if

        if (src.asFile().isFile()) {
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(src.getAbsolutePath()));
            try {
                BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dst.getAbsolutePath()));
                byte buf[] = new byte[DEFAULT_READ_BUFFER_SIZE];
                try {
                    int readBytes = in.read(buf);

                    while (readBytes >= 0) {
                        out.write(buf, 0, readBytes);
                        readBytes = in.read(buf);
                    } // while
                } finally {
                    out.close();
                }
            } finally {
                in.close();
            }
        } else {
            mkdirs(dst);
            Path contents[] = src.listPathes();
            if (contents != null) {
                for (int i = 0; i < contents.length; i++) {
                    Path newDst = dst.cat(contents[i].getName());
                    if (!copyContents(contents[i], newDst, overwrite)) {
                        return false;
                    } // if
                } // for i
            } // if
        } // else
        return true;
    }
    
    
    public static void main(String[] args) {
    	String ss = "abscd";
    	String[] s = ss.split("");
    	for (String a : s) {
    		System.out.println(a);
    	}
    	
    }

}

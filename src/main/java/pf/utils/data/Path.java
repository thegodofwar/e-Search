package pf.utils.data;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import pf.utils.XMLUtil;
import pf.utils.lock.LockStateException;
import pf.utils.lock.NamedLock;


/**
 * A data-structure representing a path.
 * 
 * This class is similar and implmemented by File, but all file-system related
 * operations have been removed.
 * 
 * Path is immutable.
 * 
 * @author changwei
 *
 */
public class Path implements Comparable<Path> {
	public static final Logger LOG=Logger.getLogger(Path.class.getName());

    /**
     * This abstract pathname's normalized pathname string.  A normalized
     * pathname string uses the default name-separator character and does not
     * contain any duplicate or redundant separators.
     *
     * @serial
     */
    private File file;

    /**
     * The system-dependent default name-separator character.  This field is
     * initialized to contain the first character of the value of the system
     * property <code>file.separator</code>.  On UNIX systems the value of this
     * field is <code>'/'</code>; on Microsoft Windows systems it is <code>'\\'</code>.
     *
     * @see     java.lang.System#getProperty(java.lang.String)
     */
    public static final char separatorChar = File.separatorChar;

    /**
     * The system-dependent default name-separator character, represented as a
     * string for convenience.  This string contains a single character, namely
     * <code>{@link #separatorChar}</code>.
     */
    public static final String separator = "" + separatorChar;

    /**
     * The system-dependent path-separator character.  This field is
     * initialized to contain the first character of the value of the system
     * property <code>path.separator</code>.  This character is used to
     * separate filenames in a sequence of files given as a <em>path list</em>.
     * On UNIX systems, this character is <code>':'</code>; on Microsoft Windows systems it
     * is <code>';'</code>.
     *
     * @see     java.lang.System#getProperty(java.lang.String)
     */
    public static final char pathSeparatorChar = File.pathSeparatorChar;

    /**
     * The system-dependent path-separator character, represented as a string
     * for convenience.  This string contains a single character, namely
     * <code>{@link #pathSeparatorChar}</code>.
     */
    public static final String pathSeparator = "" + pathSeparatorChar;

    /* -- Constructors -- */

    /**
     * Creates a new <code>Path</code> instance by converting the given
     * pathname string into an abstract pathname.  If the given string is
     * the empty string, then the result is the empty abstract pathname.
     *
     * @param   pathname  A pathname string
     * @throws  NullPointerException
     *          If the <code>pathname</code> argument is <code>null</code>
     */
    public Path(String pathname) {
        this.file = new File(pathname);
    }
    /**
     * Creates a new Path instalce with a File instance
     * @param file  the file instance
     */
    public Path(File file) {
        this.file = file;
    }

    /**
     * Creates a new <code>Path</code> instance from a parent pathname string
     * and a child pathname string.
     *
     * <p> If <code>parent</code> is <code>null</code> then the new
     * <code>File</code> instance is created as if by invoking the
     * single-argument <code>File</code> constructor on the given
     * <code>child</code> pathname string.
     *
     * <p> Otherwise the <code>parent</code> pathname string is taken to denote
     * a directory, and the <code>child</code> pathname string is taken to
     * denote either a directory or a file.  If the <code>child</code> pathname
     * string is absolute then it is converted into a relative pathname in a
     * system-dependent way.  If <code>parent</code> is the empty string then
     * the new <code>Path</code> instance is created by converting
     * <code>child</code> into an abstract pathname and resolving the result
     * against a system-dependent default directory.  Otherwise each pathname
     * string is converted into an abstract pathname and the child abstract
     * pathname is resolved against the parent.
     *
     * @param   parent  The parent pathname string
     * @param   child   The child pathname string
     * @throws  NullPointerException
     *          If <code>child</code> is <code>null</code>
     */
    public Path(String parent, String child) {
        this.file = new File(parent, child);
    }

    /**
     * Creates a new <code>Path</code> instance from a parent abstract
     * pathname and a child pathname string.
     *
     * <p> If <code>parent</code> is <code>null</code> then the new
     * <code>File</code> instance is created as if by invoking the
     * single-argument <code>File</code> constructor on the given
     * <code>child</code> pathname string.
     *
     * <p> Otherwise the <code>parent</code> abstract pathname is taken to
     * denote a directory, and the <code>child</code> pathname string is taken
     * to denote either a directory or a file.  If the <code>child</code>
     * pathname string is absolute then it is converted into a relative
     * pathname in a system-dependent way.  If <code>parent</code> is the empty
     * abstract pathname then the new <code>Path</code> instance is created by
     * converting <code>child</code> into an abstract pathname and resolving
     * the result against a system-dependent default directory.  Otherwise each
     * pathname string is converted into an abstract pathname and the child
     * abstract pathname is resolved against the parent.
     *
     * @param   parent  The parent abstract pathname
     * @param   child   The child pathname string
     * @throws  NullPointerException
     *          If <code>child</code> is <code>null</code>
     */
    public Path(File parent, String child) {
        this.file = new File(parent, child);
    }
    /**
     * Creates a new <code>Path</code> instance from a parent abstract
     * pathname and a child pathname string.
     *
     * <p> If <code>parent</code> is <code>null</code> then the new
     * <code>File</code> instance is created as if by invoking the
     * single-argument <code>File</code> constructor on the given
     * <code>child</code> pathname string.
     *
     * <p> Otherwise the <code>parent</code> abstract pathname is taken to
     * denote a directory, and the <code>child</code> pathname string is taken
     * to denote either a directory or a file.  If the <code>child</code>
     * pathname string is absolute then it is converted into a relative
     * pathname in a system-dependent way.  If <code>parent</code> is the empty
     * abstract pathname then the new <code>File</code> instance is created by
     * converting <code>child</code> into an abstract pathname and resolving
     * the result against a system-dependent default directory.  Otherwise each
     * pathname string is converted into an abstract pathname and the child
     * abstract pathname is resolved against the parent.
     *
     * @param   parent  The parent abstract pathname
     * @param   child   The child pathname string
     * @throws  NullPointerException
     *          If <code>child</code> is <code>null</code>
     */
    public Path(Path parent, String child) {
        this.file = new File(parent.file, child);
    }

    /**
     * Creates a new <tt>File</tt> instance by converting the given
     * <tt>file:</tt> URI into an abstract pathname.
     *
     * <p> The exact form of a <tt>file:</tt> URI is system-dependent, hence
     * the transformation performed by this constructor is also
     * system-dependent.
     *
     * <p> For a given abstract pathname <i>f</i> it is guaranteed that
     *
     * <blockquote><tt>
     * new File(</tt><i>&nbsp;f</i><tt>.{@link #toURI() toURI}()).equals(</tt><i>&nbsp;f</i><tt>.{@link #getAbsoluteFile() getAbsoluteFile}())
     * </tt></blockquote>
     *
     * so long as the original abstract pathname, the URI, and the new abstract
     * pathname are all created in (possibly different invocations of) the same
     * Java virtual machine.  This relationship typically does not hold,
     * however, when a <tt>file:</tt> URI that is created in a virtual machine
     * on one operating system is converted into an abstract pathname in a
     * virtual machine on a different operating system.
     *
     * @param  uri
     *         An absolute, hierarchical URI with a scheme equal to
     *         <tt>"file"</tt>, a non-empty path component, and undefined
     *         authority, query, and fragment components
     *
     * @throws  NullPointerException
     *          If <tt>uri</tt> is <tt>null</tt>
     *
     * @throws  IllegalArgumentException
     *          If the preconditions on the parameter do not hold
     *
     * @see #toURI()
     * @see java.net.URI
     * @since 1.4
     */
    public Path(URI uri) {
        this.file = new File(uri);
    }

    /* -- Path-component accessors -- */

    /**
     * Returns the name of the file or directory denoted by this abstract
     * pathname.  This is just the last name in the pathname's name
     * sequence.  If the pathname's name sequence is empty, then the empty
     * string is returned.
     *
     * @return  The name of the file or directory denoted by this abstract
     *          pathname, or the empty string if this pathname's name sequence
     *          is empty
     */
    public String getName() {
        return file.getName();
    }

    /**
     * Returns the pathname string of this abstract pathname's parent, or
     * <code>null</code> if this pathname does not name a parent directory.
     *
     * <p> The <em>parent</em> of an abstract pathname consists of the
     * pathname's prefix, if any, and each name in the pathname's name
     * sequence except for the last.  If the name sequence is empty then
     * the pathname does not name a parent directory.
     *
     * @return  The pathname string of the parent directory named by this
     *          abstract pathname, or <code>null</code> if this pathname
     *          does not name a parent
     */
    public String getParent() {
        return file.getParent();
    }

    /**
     * Returns the abstract pathname of this abstract pathname's parent,
     * or <code>null</code> if this pathname does not name a parent
     * directory.
     *
     * <p> The <em>parent</em> of an abstract pathname consists of the
     * pathname's prefix, if any, and each name in the pathname's name
     * sequence except for the last.  If the name sequence is empty then
     * the pathname does not name a parent directory.
     *
     * @return  The abstract pathname of the parent directory named by this
     *          abstract pathname, or <code>null</code> if this pathname
     *          does not name a parent
     *
     * @since 1.2
     */
    public Path getParentFile() {
        File par = file.getParentFile();
        return par == null ? null : new Path(file.getParentFile());
    }

    /**
     * Converts this abstract pathname into a pathname string.  The resulting
     * string uses the {@link #separator default name-separator character} to
     * separate the names in the name sequence.
     *
     * @return  The string form of this abstract pathname
     */
    public String getPath() {
        return file.getPath();
    }

    /**
     * Return the absolute path for the given file.
     * @see java.io.File#getAbsolutePath()
     * @return
     */
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }
    
    /* -- Path operations -- */

    /**
     * Tests whether this abstract pathname is absolute.  The definition of
     * absolute pathname is system dependent.  On UNIX systems, a pathname is
     * absolute if its prefix is <code>"/"</code>.  On Microsoft Windows systems, a
     * pathname is absolute if its prefix is a drive specifier followed by
     * <code>"\\"</code>, or if its prefix is <code>"\\\\"</code>.
     *
     * @return  <code>true</code> if this abstract pathname is absolute,
     *          <code>false</code> otherwise
     */
    public boolean isAbsolute() {
        return file.isAbsolute();
    }

    /**
     * Converts this abstract pathname into a <code>file:</code> URL.  The
     * exact form of the URL is system-dependent.  If it can be determined that
     * the file denoted by this abstract pathname is a directory, then the
     * resulting URL will end with a slash.
     *
     * <p> <b>Usage note:</b> This method does not automatically escape
     * characters that are illegal in URLs.  It is recommended that new code
     * convert an abstract pathname into a URL by first converting it into a
     * URI, via the {@link #toURI() toURI} method, and then converting the URI
     * into a URL via the {@link java.net.URI#toURL() URI.toURL} method.
     *
     * @return  A URL object representing the equivalent file URL
     *
     * @throws  MalformedURLException
     *          If the path cannot be parsed as a URL
     *
     * @see     #toURI()
     * @see     java.net.URI
     * @see     java.net.URI#toURL()
     * @see     java.net.URL
     * @since   1.2
     */
    @SuppressWarnings("deprecation")
	public URL toURL() throws MalformedURLException {
        return file.toURL();
    }

    /**
     * Constructs a <tt>file:</tt> URI that represents this abstract pathname.
     *
     * <p> The exact form of the URI is system-dependent.  If it can be
     * determined that the file denoted by this abstract pathname is a
     * directory, then the resulting URI will end with a slash.
     *
     * <p> For a given abstract pathname <i>f</i>, it is guaranteed that
     *
     * <blockquote><tt>
     * new {@link File#File(java.net.URI) File}(</tt><i>&nbsp;f</i><tt>.toURI()).equals(</tt><i>&nbsp;f</i><tt>.{@link #getAbsoluteFile() getAbsoluteFile}())
     * </tt></blockquote>
     *
     * so long as the original abstract pathname, the URI, and the new abstract
     * pathname are all created in (possibly different invocations of) the same
     * Java virtual machine.  Due to the system-dependent nature of abstract
     * pathnames, however, this relationship typically does not hold when a
     * <tt>file:</tt> URI that is created in a virtual machine on one operating
     * system is converted into an abstract pathname in a virtual machine on a
     * different operating system.
     *
     * @return  An absolute, hierarchical URI with a scheme equal to
     *          <tt>"file"</tt>, a path representing this abstract pathname,
     *          and undefined authority, query, and fragment components
     *
     * @see File#File(java.net.URI)
     * @see java.net.URI
     * @see java.net.URI#toURL()
     * @since 1.4
     */
    public URI toURI() {
        return file.toURI();
    }


    /* -- Basic infrastructure -- */

    /**
     * Compares two abstract pathnames lexicographically.  The ordering
     * defined by this method depends upon the underlying system.  On UNIX
     * systems, alphabetic case is significant in comparing pathnames; on Microsoft Windows
     * systems it is not.
     *
     * @param   pathname  The abstract pathname to be compared to this abstract
     *                    pathname
     * 
     * @return  Zero if the argument is equal to this abstract pathname, a
     *      value less than zero if this abstract pathname is
     *      lexicographically less than the argument, or a value greater
     *      than zero if this abstract pathname is lexicographically
     *      greater than the argument
     *
     * @since   1.2
     */
    public int compareTo(Path pathname) {
        return file.compareTo(pathname.file);
    }

    /**
     * Tests this abstract pathname for equality with the given object.
     * Returns <code>true</code> if and only if the argument is not
     * <code>null</code> and is an abstract pathname that denotes the same file
     * or directory as this abstract pathname.  Whether or not two abstract
     * pathnames are equal depends upon the underlying system.  On UNIX
     * systems, alphabetic case is significant in comparing pathnames; on Microsoft Windows
     * systems it is not.
     *
     * @param   obj   The object to be compared with this abstract pathname
     *
     * @return  <code>true</code> if and only if the objects are the same;
     *          <code>false</code> otherwise
     */
    public boolean equals(Object obj) {
        if ((obj != null) && (obj instanceof Path)) {
            return compareTo((Path)obj) == 0;
        } // if
        return false;
    }

    /**
     * Computes a hash code for this abstract pathname.  Because equality of
     * abstract pathnames is inherently system-dependent, so is the computation
     * of their hash codes.  On UNIX systems, the hash code of an abstract
     * pathname is equal to the exclusive <em>or</em> of the hash code
     * of its pathname string and the decimal value
     * <code>1234321</code>.  On Microsoft Windows systems, the hash
     * code is equal to the exclusive <em>or</em> of the hash code of
     * its pathname string converted to lower case and the decimal
     * value <code>1234321</code>.
     *
     * @return  A hash code for this abstract pathname
     */
    public int hashCode() {
        return file.hashCode();
    }

    /**
     * Returns the pathname string of this abstract pathname.  This is just the
     * string returned by the <code>{@link #getPath}</code> method.
     *
     * @return  The string form of this abstract pathname
     */
    public String toString() {
        return file.toString();
    }
    
    /**
     * Return a File object representing the path.
     * 
     * @return the file object
     */
    public File asFile() {
        return file;
    }
    
    /**
     * Return a new path with a sub-path catenated to the current path.
     * 
     * @param sub  the sub-path to be cantenated
     * @return  the new path
     */
    public Path cat(String sub) {
        return new Path(this, sub);
    }
    
    public Path[] listPathes() {
    	File[] files = file.listFiles();
    	Path[] pathes = new Path[files.length];
    	for (int i=0; i<files.length; i++) {
    		pathes[i] = new Path(files[i]);
    	} 
    	return pathes;
    }
    
    /**
     * Return the relative path of a give path the this path.
     * 
     * @param path
     * @return
     */
    public String relative(Path path) {
        // TODO implement this
        throw new RuntimeException("Not implemented yet!");
    }
    
    
    
    private Map<String, NamedLock<String>> locks = new HashMap<String, NamedLock<String>>();
    public static final int COMPLETE_SUCCESS = 2;
    /**
     * Obtain a filesystem lock at File f.
     */
    public synchronized void getLock(int lock) throws IOException {
      String lockname = file.getCanonicalPath();
      NamedLock<String> namedlock = locks.get(lockname);
      if (namedlock == null) {
        namedlock = new NamedLock<String>(lockname);
        locks.put(lockname, namedlock);
      }
      
      String holder = "thread-" + Thread.currentThread().hashCode();
      
      while (true) {
        switch(lock) {
        case NamedLock.SHARED_LOCK :
          if (namedlock.sharedLock(holder)) return;
          break;
        case NamedLock.UPDATE_LOCK :
          if (namedlock.updateLock(holder)) return;
          break;
        case NamedLock.EXCLUSIVE_LOCK :
          if (namedlock.exclusiveLock(holder) == 2) return;
          break;
        }
        
        try {
          this.wait(1000);
        } catch(InterruptedException e) {}
      }
    }
    /**
     * Obtain a filesystem lock at File f.
     */
    public synchronized void promoteLock() throws LockStateException, IOException {
      String lockname = file.getCanonicalPath();
      NamedLock<String> namedlock = locks.get(lockname);
      if (namedlock == null) {
        namedlock = new NamedLock<String>(lockname);
        locks.put(lockname, namedlock);
      }
      long start = System.currentTimeMillis();
      String holder = "thread-" + Thread.currentThread().hashCode();
      int rv;
      try {
          while (true) {
              rv = namedlock.promote(holder);
              if (rv == COMPLETE_SUCCESS) 
                  break;


              Thread.sleep(400);
              if (System.currentTimeMillis() - start > 5000) {
                  LOG.info("Waiting to retry promote lock for "
                          + (System.currentTimeMillis() - start) + " ms.");
                  Thread.sleep(2000);
              }

          } 
      }catch (InterruptedException ie) {
      }
    }
    
    public synchronized String getLockState() throws IOException {
      String lockName = file.getCanonicalPath();
      NamedLock<String> namedlock = locks.get(lockName);
      if (namedlock == null) {
        return NamedLock.LOCK_NAME(NamedLock.FREE);
      } else {
        return namedlock.toString();
      }
    }
    
    /**
     * Release a held lock
     */
    public synchronized void releaseLock() throws IOException {
      String lockname = file.getCanonicalPath();
      NamedLock<String> namedlock = locks.get(lockname);
      if (namedlock == null) return;
      String holder = "thread-" + Thread.currentThread().hashCode();
      namedlock.unlock(holder);
    }
}
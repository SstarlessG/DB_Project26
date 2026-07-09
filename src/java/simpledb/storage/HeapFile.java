package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here 
        int pageSize = BufferPool.getPageSize();
        byte[] pageData = new byte[pageSize];
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek((long) pid.getPageNumber() * pageSize);
            raf.readFully(pageData);
            raf.close();
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            throw new IllegalArgumentException();
        }   
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // Get the page size and offset of the page to write
        int pageSize = BufferPool.getPageSize();
        int pageOffset = page.getId().getPageNumber() * pageSize;

        // Open the file for writing
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(pageOffset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // Iterate through the pages to find a page with empty slots
        ArrayList<Page> dirtyPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(), i); // i is page no.
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            
            // If the page has empty slots, insert the tuple and mark the page as dirty
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                dirtyPages.add(page);
                return dirtyPages;
            }
        }
        
        // If no page has empty slots, create a new page and insert the tuple
        HeapPageId newPid = new HeapPageId(getId(), numPages());

        // Create a new empty page on disk
        writePage(new HeapPage(newPid, HeapPage.createEmptyPageData()));
        HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        dirtyPages.add(newPage);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // Get the page containing the tuple to delete
        HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        
        // delete and return the page
        page.deleteTuple(t);
        ArrayList<Page> dirtyPages = new ArrayList<>();
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int currentPageIndex = 0;
            private Iterator<Tuple> currentTupleIterator = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                currentPageIndex = 0;
                if (numPages() > 0) {
                    HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    currentTupleIterator = page.iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currentTupleIterator == null) {
                    return false;
                }
                if (currentTupleIterator.hasNext()) {
                    return true;
                } else {
                    while (++currentPageIndex < numPages()) {
                        HeapPageId pid = new HeapPageId(getId(), currentPageIndex);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                        currentTupleIterator = page.iterator();
                        if (currentTupleIterator.hasNext()) {
                            return true;
                        }
                    }
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return currentTupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                currentTupleIterator = null;
                currentPageIndex = 0;
            }
        };
    }
}


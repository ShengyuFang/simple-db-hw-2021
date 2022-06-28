package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.index.BTreeLeafPage;
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

    /**
     * file是binary page file
     * HeapFileEncoder
     */
    private File file;

    private TupleDesc td;

    private long fileLength;
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
        fileLength = file.length();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
    //Distributed id is not needed
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page heapPage;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.read(data);
            heapPage = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return heapPage;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(fileLength / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        private TransactionId tid;

        private Iterator<Tuple> iterator;

        private int pageNo;

        public HeapFileIterator (TransactionId tid) {
            this.tid = tid;
            this.pageNo = 0;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId pageId = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            iterator = page.iterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator != null && !iterator.hasNext()) {
                iterator = null;
            }
            while(iterator == null && (++pageNo) * BufferPool.getPageSize() < fileLength) {
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNo), Permissions.READ_ONLY);
                iterator = page.iterator();
                if (!iterator.hasNext()) {
                    iterator = null;
                }
            }
            if(iterator == null) {
                return null;
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            iterator = null;
            pageNo = 0;
        }
    }

}


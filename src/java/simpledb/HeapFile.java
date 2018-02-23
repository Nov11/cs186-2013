package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
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
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (!(pid instanceof HeapPageId)) {
            return null;
        }
        HeapPageId heapPageId = (HeapPageId) pid;
        int pageNumber = pid.pageNumber();
        int offset = pageNumber * BufferPool.PAGE_SIZE;
        byte[] buffer = new byte[BufferPool.PAGE_SIZE];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");) {
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer);
            HeapPage page = new HeapPage(heapPageId, buffer);
            return page;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        int num = page.getId().pageNumber();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek(num * BufferPool.PAGE_SIZE);
            randomAccessFile.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        return (int) file.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        BufferPool bufferPool = Database.getBufferPool();
        HeapPageId heapPageId = null;

        for (int i = 0; i < numPages(); i++) {
            HeapPageId phid = new HeapPageId(getId(), i);
            Page page = bufferPool.getPage(tid, phid, Permissions.READ_ONLY);
            assert page != null;
            assert page instanceof HeapPage;
            HeapPage hp = (HeapPage) page;
            if (hp.getNumEmptySlots() > 0) {
                heapPageId = phid;
                break;
            }
        }
        if (heapPageId == null) {
            heapPageId = new HeapPageId(getId(), numPages());
            HeapPage hp = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            this.writePage(hp);
        }
        HeapPage heapPage = (HeapPage) bufferPool.getPage(tid, heapPageId, Permissions.READ_WRITE);
        heapPage.markDirty(true, tid);
        int tmp = heapPage.getNumEmptySlots();
        heapPage.insertTuple(t);
        ArrayList<Page> result = new ArrayList<>();
        result.add(heapPage);
        return result;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        BufferPool bufferPool = Database.getBufferPool();
        HeapPage heapPage = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.markDirty(true, tid);
        heapPage.deleteTuple(t);
        return heapPage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileTupleIterator(this, tid);
    }

    static class HeapFileTupleIterator implements DbFileIterator {
        private HeapFile hf;
        private TransactionId tid;

        private int pageIndex = 0;
        private boolean opened = false;

        private Iterator<Tuple> iterator;

        public HeapFileTupleIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }

        private void loadNextPage() throws DbException, TransactionAbortedException {
            BufferPool bufferPool = Database.getBufferPool();
            PageId pageId = new HeapPageId(hf.getId(), pageIndex++);
            HeapPage curPage = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_ONLY);
            iterator = curPage.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageIndex = 0;
            loadNextPage();
            opened = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!opened) {
                return false;
            }
            if (iterator != null && iterator.hasNext()) {
                return true;
            }
            while ((iterator == null || !iterator.hasNext()) && pageIndex < hf.numPages()) {
                loadNextPage();
            }
            return iterator != null && iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException("no more tuples");
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
            opened = false;
        }
    }
}


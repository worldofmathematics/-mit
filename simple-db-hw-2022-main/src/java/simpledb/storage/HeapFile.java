package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
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
        // TODO: some code goes here
        //throw new UnsupportedOperationException("implement this");
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        //throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs

    /**
     * Push the specified page to disk.
     *
     * @param pid The page to write.  page.getId().pageno() specifies the offset into
     *            the file where the page should be written.
     * @throws IOException if the write fails
     */
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile file = null;
        try{
            file= new RandomAccessFile(f,"r");
            if((long)(pgNo+1)*BufferPool.getPageSize() > f.length()){
                file.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            file.seek(pgNo * BufferPool.getPageSize());
            // big end
            int read = file.read(bytes,0,BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
            return new HeapPage(id,bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                file.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        int r = (int) Math.floor(getFile().length() * 1.0 / BufferPool.getPageSize());
        return r;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;
        // Ԫ�������
        private Iterator<Tuple> iterator;
        private int readnum;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // ��ȡ��һҳ��ȫ��Ԫ��
            readnum = 0;
            iterator = getPageTuple(readnum);
        }

        // ��ȡ��ǰҳ��������
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
            // ���ļ���Χ��
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid=new HeapPageId(heapFile.getId(),pageNumber);
                // �ӻ�����в�ѯ��Ӧ��ҳ�� ��Ȩ��
                HeapPage page =(HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // ���������Ϊ��
            if (iterator == null) {
                return false;
            }
            // ����Ѿ���������
            if (!iterator.hasNext()) {
                // �Ƿ񻹴�����һҳ��С���ļ������ҳ
                while (readnum < (heapFile.numPages() - 1)) {
                    readnum++;
                    // ��ȡ��һҳ
                    iterator = getPageTuple(readnum);
                    if (iterator.hasNext()) {
                        return iterator.hasNext();
                    }
                }
                // ����Ԫ���ȡ���
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // ���û��Ԫ���ˣ��׳��쳣
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            // ������һ��Ԫ��
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // �����һ��������
            close();
            // ���¿�ʼ
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

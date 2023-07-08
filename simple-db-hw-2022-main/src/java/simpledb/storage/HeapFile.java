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
        try {
            file = new RandomAccessFile(f, "r");
            if ((long) (pgNo + 1) * BufferPool.getPageSize() > f.length()) {
                file.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            file.seek(pgNo * BufferPool.getPageSize());
            // big end
            int read = file.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        RandomAccessFile file = null;
        file = new RandomAccessFile(f, "rw");
        if (pgNo > numPages()) {
            throw new IllegalArgumentException();
        }
        int pageSiz = BufferPool.getPageSize();
        file.seek(pageSiz * pgNo);
        byte[] data = page.getPageData();
        file.write(data);
        file.close();
        page.markDirty(false, null);
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

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t   The tuple to add.  This tuple should be updated to reflect that
     *            it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     *                     如果一个事务 t 在页 p 上找不到空闲槽， t 可能会立即释放对 p 的锁
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        // TODO: some code goes here
        // not necessary for lab1
        List<Page> pageList = new ArrayList<>();
        //有空闲页可以进行插入
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pid = new HeapPageId(getId(),i);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage
                    (tid, pid, Permissions.READ_WRITE);
            if (p.getNumUnusedSlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid,pid);
                continue;
            }
            p.insertTuple(t);
            pageList.add(p);
            return pageList;

        }
        //没有空闲也可以插入
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));//????查阅借鉴明天看书学习
        byte[] b = HeapPage.createEmptyPageData();
        bw.write(b);
        bw.close();
        HeapPage p = (HeapPage) Database.getBufferPool().getPage
                (tid, new HeapPageId(this.getId(), numPages() - 1), Permissions.READ_WRITE);
        p.insertTuple(t);
        pageList.add(p);
        return pageList;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true, tid);
        return Collections.singletonList(p);//？？？
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
        // 元组迭代器
        private Iterator<Tuple> iterator;
        private int readnum;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // 获取第一页的全部元组
            readnum = 0;
            iterator = getPageTuple(readnum);
        }

        // 获取当前页的所有行
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
            // 在文件范围内
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid=new HeapPageId(heapFile.getId(),pageNumber);
                // 从缓存池中查询相应的页面 读权限
                HeapPage page =(HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
            }

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // 如果迭代器为空
            if (iterator == null) {
                return false;
            }
            // 如果已经遍历结束
            if (!iterator.hasNext()) {
                // 是否还存在下一页，小于文件的最大页
                while (readnum < (heapFile.numPages() - 1)) {
                    readnum++;
                    // 获取下一页
                    iterator = getPageTuple(readnum);
                    if (iterator.hasNext()) {
                        return iterator.hasNext();
                    }
                }
                // 所有元组获取完毕
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 如果没有元组了，抛出异常
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            // 返回下一个元组
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 清除上一个迭代器
            close();
            // 重新开始
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}


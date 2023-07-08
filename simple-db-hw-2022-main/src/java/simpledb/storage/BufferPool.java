package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private final Page[] buffer;
    private int numPages;
    private final Map<PageId,Page> page_store;
    private LockManager lockManager;
    private LRUCache lruCache;

    /**
     * 页面的锁
     */
    private class PageLock{
        private TransactionId tid;
        private int locktype;//0为共享锁，1为互斥锁

        public PageLock(TransactionId tid, int locktype) {
            this.tid = tid;
            this.locktype = locktype;
        }

        public TransactionId getTid() {
            return tid;
        }

        public void setTid(TransactionId tid) {
            this.tid = tid;
        }

        public int getLocktype() {
            return locktype;
        }

        public void setLocktype(int locktype) {
            this.locktype = locktype;
        }
    }

    /**
     * 锁的管理，管理加锁和释放锁
     *       1.申请锁
     *       2.释放锁
     *       3.判断指定事务是否持有某一page上的锁
     */
    private class LockManager{
        ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockMap;
        public LockManager(){
            this.lockMap=new ConcurrentHashMap<>();
        }

        /**
         * Return true if the specified transaction has a lock on the specified page
         * @param tid 需要进行判断的事务
         * @param p 需要进行判断的page
         * @return
         */
        public boolean holdsLock(TransactionId tid, PageId p) {//判断指定事务是否持有某一page上的锁
            // TODO: some code goes here
            // not necessary for lab1|lab2
            ConcurrentHashMap<TransactionId,PageLock> pagelocks;
            pagelocks=lockMap.get(p);
            if(pagelocks==null)
            {
                return false;
            }
           for(TransactionId t:pagelocks.keySet())
           {
               if(t==tid)
               {
                   return true;
               }
           }
           return false;
        }

        /**
         * 释放指定事务指定页上的锁
         * @param tid 需要进行释放锁操作的事务
         * @param pid 需要进行释放锁的页
         */
        public synchronized void releaselock(TransactionId tid,PageId pid){
            if(holdsLock(tid,pid)){
                ConcurrentHashMap<TransactionId,PageLock> pagelocks;
                pagelocks=lockMap.get(pid);
                pagelocks.remove(tid);
                if(pagelocks.size()==0)
                {
                    lockMap.remove(pid);
                }
            }
            this.notify();
        }

        /**
         * 某个事务某页申请加锁
         * @param tid 申请加锁的事务
         * @param pid 申请加锁的页
         * @param lockType 申请加锁的类型
         * @return
         * 加锁原理
         * 1.在一个事务可以读一个对象之前，它必须在对象上有一个共享锁。
         * 2.在事务可以写对象之前，它必须对对象具有互斥锁。
         * 3.多个事务可以对一个对象有一个共享锁。
         * 4.只有一个事务可以对一个对象拥有互斥锁。
         * 5.如果事务 t 是唯一持有对象上的共享锁的事务，那么事务 t 可以将其对对象  的锁升级为互斥锁
         * 具体实现：1.先判断该页是否有锁，如果没有锁直接进行加锁操作，并将所加锁的信息加入map中
         *         2.该页面上确定有锁，分两种情况，一种是事务tid上有锁，一种是事务tid上没锁
         *         2.1tid上有锁，   申请的是S锁：直接授予
         *                        申请的是X锁，如果t是唯一持有共享锁的事务，则进行锁升级；
         *                        否则，不能进行升级，可能出现死锁，等待/抛出异常
         *          2.2不是t上的锁，申请的是S锁，如果只有S锁，直接获取；如果存在X锁，等待/抛出异常
         *                        申请的是X锁，等待，抛出异常
         */
        //0为S锁，1为X锁
        // ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockMap;
        public synchronized boolean getLocks(TransactionId tid,PageId pid,int lockType ) throws InterruptedException, TransactionAbortedException {
            final String thread = Thread.currentThread().getName();
            if (lockMap.get(pid) == null) {
                return putLock(tid, pid, lockType);
            }

            //获取页面上的锁
            ConcurrentHashMap<TransactionId, PageLock> page_lock = lockMap.get(pid);
            //判断是否为申请事务tid上的锁
            //没有事务tid上的锁
            if (page_lock.get(tid) == null) {
                if (lockType == 1)//申请X锁
                {
                    wait(10);
                    System.out.println(thread + ": the " + pid + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                    return false;
                } else if (lockType == 0) {//申请S锁
                    if (page_lock.size() > 1)//页面上的锁的数量大于1说明只有S锁
                    {
                        return putLock(tid, pid, lockType);
                    } else if (page_lock.size() == 1) {
                        Collection<PageLock> p = page_lock.values();
                        for (PageLock value : p) {
                            if (value.getLocktype() == 0)//如果有的一个锁为S锁
                            {
                                return putLock(tid, pid, lockType);
                            } else {
                                wait(10);
                                System.out.println(thread + ": the " + pid + " have one write lock with diff txid, transaction" + tid + " require read lock, await...");
                                return false;
                            }
                        }

                    }
                }
            } else if (page_lock.get(tid) != null) {//事务tid上有锁
                if (lockType == 0) {//申请的为S锁
                    //System.out.println("Succeed!");
                    return true;
                } else {//申请的为X锁
                    if (page_lock.get(tid).getLocktype() == 1) {
                        return true;
                    }
                    if(page_lock.size()>1)
                    {
                        System.out.println(thread + ": the " + pid + " have many read locks, transaction" + tid + " require write lock, abort!!!");
                        throw new TransactionAbortedException();
                    }
                    if(page_lock.get(tid).getLocktype()==0&&page_lock.size()==1){
                        //System.out.println("Succeed!");
                        page_lock.get(tid).setLocktype(1);
                        page_lock.put(tid, page_lock.get(tid));
                        return  true;
                    }
                }
            }
            return false;
        }
        public boolean putLock(TransactionId tid, PageId pid,int lockType){
            PageLock pagelocks=new PageLock(tid,lockType);
            ConcurrentHashMap<TransactionId,PageLock> p=lockMap.get(pid);
            if(p==null)
            {
                p=new ConcurrentHashMap<>();
                lockMap.put(pid,p);
            }
            p.put(tid,pagelocks);
            lockMap.put(pid,p);
            //System.out.println("Succeed!");
            return true;
        }
    }
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        buffer = new Page[numPages];
        this.page_store=new HashMap<>();
        this.lockManager=new LockManager();
        this.lruCache=new LRUCache(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)//不太适用复杂情况事务ID。
    // TransactionId 如果对象不是事务，则此值为mull。
            throws TransactionAbortedException, DbException {
        /*int idx = -1;
        for (int i = 0; i < buffer.length; ++i) {
            if (null == buffer[i]) {//如果缓冲池没有且buffer[i]为空，记录缓冲块位置
                idx = i;
            } else if (pid.equals(buffer[i].getId())) {
                return buffer[i];//缓冲池中有直接返回页
            }
        }
        //去disk找将buffer[i]读入
        return buffer[idx] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);*/
        int lockType;
        if (perm == Permissions.READ_ONLY){
            lockType =0;
        } else {
            lockType = 1;
        }
        long st = System.currentTimeMillis();
        boolean isacquired = false;
        while(!isacquired){

            try {
                isacquired = lockManager.getLocks(tid,pid,lockType);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if(now - st > 300){
                throw new TransactionAbortedException();
            }
        }

        /*try {
            if(lockManager.getLocks(tid,pid,lockType,0))
            {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/
        if (lruCache.get(pid) == null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            lruCache.put(pid, page);
        }
        if(lruCache.get(pid)==null)
        {
            throw new DbException("该页不存在");
        }
        return lruCache.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.releaselock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *so can simply be implemented by calling transactionComplete(tid, true).
     * 直接调用函数
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }


    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     * When you commit, you should flush dirty pages associated to the transaction to disk.
     *  When you abort, you should revert any changes made by the transaction by restoring
     *  the page to its on-disk state.
     *  所以需要定义一个新的函数进行revert操作
     *  无论事务提交还是终止，您都应该释放 BufferPool 保持的有关事务的任何状态，包括释放
     * 事务持有的任何锁。
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        if(commit)
        {
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else{
            revert(tid);
        }
        for(PageId pid:lruCache.cache.keySet())
        {
            if(lockManager.holdsLock(tid,pid))
            {
                lockManager.releaselock(tid,pid);
            }
        }
    }
    public void revert(TransactionId tid){
        for(Map.Entry<PageId, LRUCache.DLinkNode> group : lruCache.cache.entrySet())
        {
            PageId p=group.getKey();
            Page pages=group.getValue().value;
            if(tid.equals(pages.isDirty()))
            {
                int tableId=p.getTableId();
                Page page=Database.getCatalog().getDatabaseFile(tableId).readPage(p);
                lruCache.removeNode(group.getValue());
                try {
                    lruCache.put(p,page);
                } catch (DbException e) {
                    throw new RuntimeException(e);
                }
            }

        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        /*boolean in=false;
        for (Page page : p) {
            page.markDirty(true, tid);
            if(page_store.size()<numPages)//buffer里有空闲位置
            {
                in=true;
            }
            for(int i=0;i<numPages;i++)
            {
                if(buffer[i]==page.getId())//该页已经缓存到buffer中
                {
                    in=true;
                }
            }
            if(in)//可以插入
            {
               lruCache.put(page.getId(),page);
            }
            else//此时说明buffer已经满了
            {
                evictPage();
                lruCache.put(page.getId(),page);
            }
        }*/
        /*List<Page> p=Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid,t);//数组列表包含已修改的页面
        for(Page pages:p)
        {
            pages.markDirty(true,tid);
            lruCache.put(pages.getId(),pages);
        }*/
        //System.out.println("InsertTuple");
        //System.out.println(tableId);
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(f.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
       /* List<Page> p=Database.getCatalog().getDatabaseFile
                (t.getRecordId().getPageId().getTableId()).deleteTuple(tid,t);//数组列表包含已修改的页面
        for (Page page : p) {
            page.markDirty(true, tid);
        }*/
        DbFile updateFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> updatePages = updateFile.deleteTuple(tid, t);
        updateBufferPool(updatePages, tid);
    }
    public void updateBufferPool(List<Page> updatePages, TransactionId tid) {
        for (Page page : updatePages) {
            page.markDirty(true, tid);
            // update bufferPool
            try {
                lruCache.put(page.getId(), page);
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        }
    }
    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        /*for(Page p:page_store.values()){
            flushPage(p.getId());
        }*/
        for (Map.Entry<PageId, LRUCache.DLinkNode> group : lruCache.cache.entrySet()) {
            Page page = group.getValue().value;
            if (page.isDirty() != null) {
                this.flushPage(group.getKey());
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        LRUCache.DLinkNode node=lruCache.cache.get(pid);
        lruCache.removeNode(node);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        /*Page p=page_store.get(pid);
        Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
        p.markDirty(false,null);*/
        Page p=lruCache.get(pid);
        Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
        p.markDirty(false,null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, LRUCache.DLinkNode> group : lruCache.cache.entrySet()){
            PageId pid=group.getKey();
            Page p=group.getValue().value;
            if(tid.equals(p.isDirty()))
            {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    /**
     *
     *LRU 算法 <a href="https://juejin.cn/post/7027062270702125093">...</a>
     * LRU算法实现需要一个双向链表和哈希表
     * 实现一个LRU Cache,该Cache可以满足查找一个数据，删除一个数据，添加一个数据
     * 查找一个数据：散列表中查找数据的时间复杂度接近 O(1)，所以通过散列表，我们可以很快地在缓存
     *            中找到一个数据。当找到数据之后，我们还需要将它移动到双向链表的尾部。
     *删除一个数据：在散列表中根据映射关系，通过双向链表删除想要删除的数据
     *添加一个数据：我们需要先看这个数据是否已经在缓存中。如果已经在其中，需要将其移动到双向链表
     *           的尾部；如果不在其中，还要看缓存有没有满。如果满了，则将双向链表头部的结点删除，
     *           然后再将数据放到链表的尾部；如果没有满，就直接将数据放到链表的尾部。
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    public class LRUCache{
        /**
         * 双向链表节点
         */
        class DLinkNode{
            PageId key;
            Page value;
            DLinkNode prev;
            DLinkNode next;
            public DLinkNode(){};
            public DLinkNode(PageId _key, Page _value)
            {
                this.key=_key;
                this.value=_value;
            }
        }
        /**
         * 哈希表
         */
        private Map<PageId, DLinkNode> cache;
        private int size;
        private int capacity;
        private DLinkNode head=new DLinkNode(null ,null);
        private DLinkNode tail=new DLinkNode(null,null);
        /**
         * 双向链表
         */
        public LRUCache(int capacity)
        {
            size=0;
            this.capacity=capacity;
            cache=new HashMap<>();
            this.head=new DLinkNode();
            this.head=new DLinkNode();
            head.next = tail;
            tail.prev = head;
        }
        /**
         * get
         */
        public Page get(PageId key)
        {
            if(cache.containsKey(key)){
                removeTohead(cache.get(key));
                return cache.get(key).value ;
            }else{
                return null;
            }


        }
        /**
         * put
         */
        public void put(PageId key,Page value) throws DbException{
            DLinkNode node=cache.get(key);
            //如果key不在创建一个新的节点
            DLinkNode newnode=new DLinkNode(key,value);
            //System.out.println(node==null);
            if(node==null)
            {
                size++;
                if(size>capacity)
                {
                    DLinkNode removeNode=tail.prev;
                    while (removeNode.value.isDirty()!=null)
                    {
                        removeNode=removeNode.prev;
                        if(removeNode==head||removeNode==tail) {
                        throw new DbException("都是脏页");
                    }
                    }
                    cache.remove(removeNode.key);
                    removeNode(removeNode);
                    size--;
                }
                //System.out.print("size<cap");
            }else {
                //System.out.println("else执行了");
                //System.out.println("node is not null");
                removeNode(node);
            }
            //添加进哈希表
            cache.put(key,newnode);
            //添加至链表头部
            addTohead(newnode);



        }
        /**
         * 添加节点至双向链表头部
         */
        public void addTohead(DLinkNode node){
            node.prev=head;
            node.next=head.next;
            head.next.prev=node;
            head.next=node;
        }
        /**
         * 删除节点
         */
        public void removeNode(DLinkNode node){
            node.prev.next=node.next;
            node.next.prev=node.prev;
        }
        /**
         * 将节点移动到头部
         */
        public void removeTohead(DLinkNode node){
            removeNode(node);
            addTohead(node);
        }
        /**
         * 删除尾节点并返回

        public DLinkNode removetail(){
            DLinkNode p=tail.prev;
            removeNode(p);
            return p;
        }*/
    }
}




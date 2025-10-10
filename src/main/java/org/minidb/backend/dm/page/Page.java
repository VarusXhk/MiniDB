package org.minidb.backend.dm.page;

public interface Page {
    //页面锁
    void lock();
    void unlock();
    //释放一个页面
    void releasePage();
    //标志页面为脏
    void setPageDirty(boolean dirty);
    boolean isDirty();
    //获取页面数据
    int getPageNumber();
    byte[] getPageData();
}

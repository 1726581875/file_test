package com.moyu.xmz.store.accessor;

import com.moyu.xmz.common.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2024/2/19
 */
public abstract class BaseAccessor {

    protected FileAccessor fileAccessor;

    protected String filePath;

    public BaseAccessor(String filePath) throws IOException {
        this.fileAccessor = new FileAccessor(filePath);
        this.filePath = filePath;
        initFileIfNotExists(this.filePath);
    }


    public void close() {
        if(this.fileAccessor != null) {
            this.fileAccessor.close();
        }
    }

    protected void initFileIfNotExists(String filePath) throws IOException {
        File dbFile = new File(filePath);
        FileUtils.createDirIfNotExists(dbFile.getParent());
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
    }

}

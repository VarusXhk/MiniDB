package org.minidb.common.Result;

import org.minidb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileResults {

    private FileChannel fc;

    private RandomAccessFile raf;

    public FileResults(FileChannel fileChannel, RandomAccessFile randomAccessFile) {
        this.fc = fileChannel;
        this.raf = randomAccessFile;
    }

    public FileChannel getFileChannel() {
        return fc;
    }
    public RandomAccessFile getRandomAccessFile() {
        return raf;
    }

    /**
     * 向空文件写入文件头
     * @param array
     */
    public void WriteHeader(byte[] array) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        try {
            this.fc.position(0);
            this.fc.write(buffer);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}

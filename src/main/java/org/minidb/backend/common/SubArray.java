package org.minidb.backend.common;

public class SubArray {
    public int start;
    public int end;
    public byte[] data;

    public SubArray(int start, int end, byte[] data) {
        this.start = start;
        this.end = end;
        this.data = data;
    }
}

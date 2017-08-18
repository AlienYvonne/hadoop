/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.compress.isal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * A {@link Decompressor} based on the isal compression algorithm.
 */
public class IsalDecompressor implements Decompressor {
    private static final Log LOG =
            LogFactory.getLog(IsalDecompressor.class.getName());
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

    private long stream;
    private int directBufferSize;
    private int BufferSize;
    private Buffer compressedDirectBuf = null;
    private int compressedDirectBufLen;
    private int compressedDirectBufOff;
    private Buffer uncompressedDirectBuf = null;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finished;
    private CompressionGzipFlag gzipFlag;

    private static long bytesRead = 0;
    private static long bytesWritten = 0;

    private static int crc_flag = 0;

    public static enum CompressionGzipFlag{
        IGZIP_DEFLATE(0),

        IGZIP_GZIP(1),

        IGZIP_GZIP_NO_HDR(2);

        private final int gzipflag;

        CompressionGzipFlag(int gzipflag) {this.gzipflag = gzipflag;}

        public int getGzipflag(){return gzipflag;}
    }

    private static boolean nativeIsalLoaded = false;

    static {
        if (NativeCodeLoader.isNativeCodeLoaded() &&
                NativeCodeLoader.buildSupportsIsal()) {
            try {
                initIDs();
                nativeIsalLoaded = true;
            } catch (Throwable t) {
                LOG.error("failed to load IsalDecompressor/n", t);
            }
        }
    }

    public static boolean isNativeIsalLoaded() {
        return nativeIsalLoaded;
    }

    /**
     * Creates a new compressor.
     *
     * @param directBufferSize size of the direct buffer to be used.
     */
    public IsalDecompressor(CompressionGzipFlag gzipFlag,int directBufferSize) {
        this.directBufferSize = directBufferSize;
        this.BufferSize = directBufferSize;
        this.gzipFlag = gzipFlag;

        compressedDirectBuf = ByteBuffer.allocateDirect(BufferSize);
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);

        stream = init(gzipFlag.getGzipflag());

    }

    /**
     * Creates a new decompressor with the default buffer size.
     */
    public IsalDecompressor() {
        this(CompressionGzipFlag.IGZIP_DEFLATE,DEFAULT_DIRECT_BUFFER_SIZE);
    }

    /**
     * Creates a new decompressor with the configuration.
     */
    public IsalDecompressor(Configuration conf) {
        this(CompressionGzipFlag.IGZIP_DEFLATE, IsalFactory.getDirectBufferSize(conf)
                );
    }

    /**
     * Sets input data for decompression.
     * This should be called if and only if {@link #needsInput()} returns
     * <code>true</code> indicating that more input data is required.
     * (Both native and non-native versions of various Decompressors require
     * that the data passed in via <code>b[]</code> remain unmodified until
     * the caller is explicitly notified--via {@link #needsInput()}--that the
     * buffer may be safely modified.  With this requirement, an extra
     * buffer-copy can be avoided.)
     *
     * @param b   Input data
     * @param off Start offset
     * @param len Length
     */
    @Override
    public void setInput(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        this.userBuf = b;
        this.userBufOff = off;
        this.userBufLen = len;

        setInputFromSavedData();

        bytesRead += len;

        // Reinitialize isal's output direct-buffer
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    void setInputFromSavedData() {
        compressedDirectBufOff = 0;
        compressedDirectBufLen = Math.min(userBufLen, directBufferSize);

        // Reinitialize isal's input direct buffer
        compressedDirectBuf.rewind();
        ((ByteBuffer) compressedDirectBuf).put(userBuf, userBufOff,
                compressedDirectBufLen);

        // Note how much data is being fed to isal
        userBufOff += compressedDirectBufLen;
        userBufLen -= compressedDirectBufLen;
    }

    /**
     * Does nothing.
     */
    @Override
    public void setDictionary(byte[] b, int off, int len) {
        // do nothing
    }

    /**
     * Returns true if the input data buffer is empty and
     * {@link #setInput(byte[], int, int)} should be called to
     * provide more input.
     *
     * @return <code>true</code> if the input data buffer is empty and
     *         {@link #setInput(byte[], int, int)} should be called in
     *         order to provide more input.
     */
    @Override
    public boolean needsInput() {
        // Consume remaining compressed data?
        /*if(finished)
            return true;*/

        if (uncompressedDirectBuf.remaining() > 0) {
            return false;
        }

        // Check if isal has consumed all input
        if (compressedDirectBufLen <= 0) {
            // Check if we have consumed all user-input
            if (userBufLen <= 0) {
                return true;
            } else {
                setInputFromSavedData();
            }
        }

        return false;
    }

    /**
     * Returns <code>false</code>.
     *
     * @return <code>false</code>.
     */
    @Override
    public boolean needsDictionary() {
        return false;
    }

    /**
     * Returns true if the end of the decompressed
     * data output stream has been reached.
     *
     * @return <code>true</code> if the end of the decompressed
     *         data output stream has been reached.
     */
    @Override
    public boolean finished() {
        return (finished && uncompressedDirectBuf.remaining() == 0);
    }

    /**
     * Fills specified buffer with uncompressed data. Returns actual number
     * of bytes of uncompressed data. A return value of 0 indicates that
     * {@link #needsInput()} should be called in order to determine if more
     * input data is required.
     *
     * @param b   Buffer for the compressed data
     * @param off Start offset of the data
     * @param len Size of the buffer
     * @return The actual number of bytes of compressed data.
     * @throws IOException
     */
    @Override
    public int decompress(byte[] b, int off, int len)
            throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }


        int n = 0;

        // Check if there is uncompressed data
        n = uncompressedDirectBuf.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
            bytesWritten += n;
            return n;
        }

        uncompressedDirectBuf.rewind();
        uncompressedDirectBuf.limit(directBufferSize);

        // Decompress data
        n = inflateBytesDirect();

        uncompressedDirectBuf.limit(n);

        n = Math.min(n, len);
        ((ByteBuffer) uncompressedDirectBuf).get(b, off, n);
        //}
        bytesWritten += n;

        return n;
    }


    /**
     * Returns <code>0</code>.
     *
     * @return <code>0</code>.
     */
    @Override
    public int getRemaining() {
        checkStream();
        // Never use this function in BlockDecompressorStream.
        return userBufLen + getRemaining(stream);
    }

    @Override
    public void reset() {
        checkStream();
        reset(stream);
        finished = false;
        compressedDirectBufLen = compressedDirectBufOff = 0;
        uncompressedDirectBuf.limit(directBufferSize);
        uncompressedDirectBuf.position(directBufferSize);
        userBufOff = userBufLen = 0;
        bytesRead = bytesWritten = 0L;
    }

    /**
     * Resets decompressor and input and output buffers so that a new set of
     * input data can be processed.
     */

    /**
     * Return number of bytes given to this compressor since last reset.
     */

    public long getBytesRead() {
        checkStream();
        //return getBytesRead(stream);
        return bytesRead;
    }

    /**
     * Return number of bytes consumed by callers of compress since last reset.
     */

    public long getBytesWritten() {

        checkStream();
        return getBytesWritten(stream);
    }


    @Override
    public void end() {
        if(stream != 0){
            end(stream);
            stream = 0;
        }

    }

    private void checkStream(){
        if(stream == 0)
            throw new NullPointerException();
    }

    private native static void initIDs();
    private native static long init(int gzip_flag);
    private native int inflateBytesDirect();
    private native static long getBytesRead(long strm);
    private native static long getBytesWritten(long strm);
    private native static int getRemaining(long strm);
    private native static void reset(long strm);
    private native static void end(long strm);


    public native static String getLibraryName();

    int inflateDirect(ByteBuffer src, ByteBuffer dst) throws IOException {
        assert (this instanceof IsalDecompressor.IsalDirectDecompressor);

        ByteBuffer presliced = dst;
        if (dst.position() > 0) {
            presliced = dst;
            dst = dst.slice();
        }

        Buffer originalCompressed = compressedDirectBuf;
        Buffer originalUncompressed = uncompressedDirectBuf;
        int originalBufferSize = BufferSize;
        compressedDirectBuf = src.slice();
        compressedDirectBufLen = src.remaining();
        compressedDirectBufOff = src.position();
        uncompressedDirectBuf = dst;
        directBufferSize = dst.remaining();
        int n = 0;
        try {
            n = inflateBytesDirect();
            presliced.position(presliced.position() + n);
            /*// SNAPPY always consumes the whole buffer or throws an exception
            src.position(src.limit());
            finished = true;*/
            if (compressedDirectBufLen > 0) {
                src.position(compressedDirectBufOff);
            } else {
                src.position(src.limit());
            }
        } finally {
            compressedDirectBuf = originalCompressed;
            uncompressedDirectBuf = originalUncompressed;
            compressedDirectBufLen = 0;
            compressedDirectBufOff = 0;
            BufferSize = originalBufferSize;
        }
        return n;
    }

    public static class IsalDirectDecompressor extends IsalDecompressor implements
            DirectDecompressor {

        @Override
        public boolean finished() {
            return (endOfInput && super.finished());
        }

        @Override
        public void reset() {
            super.reset();
            endOfInput = true;
        }

        private boolean endOfInput;

        @Override
        public void decompress(ByteBuffer src, ByteBuffer dst)
                throws IOException {
            assert dst.isDirect() : "dst.isDirect()";
            assert src.isDirect() : "src.isDirect()";
            assert dst.remaining() > 0 : "dst.remaining() > 0";
            this.inflateDirect(src, dst);
            endOfInput = !src.hasRemaining();
        }

        @Override
        public void setDictionary(byte[] b, int off, int len) {
            throw new UnsupportedOperationException(
                    "byte[] arrays are not supported for DirectDecompressor");
        }

        @Override
        public int decompress(byte[] b, int off, int len) {
            throw new UnsupportedOperationException(
                    "byte[] arrays are not supported for DirectDecompressor");
        }
    }
}

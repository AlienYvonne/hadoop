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
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;


/**
 * A {@link Compressor} based on the isal compression algorithm.
 */
public class IsalCompressor implements Compressor {
    private static final Log LOG =
            LogFactory.getLog(IsalCompressor.class.getName());
    private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

    private long stream; // pointer for deflate state
    private int directBufferSize;
    private int BufferSize;
    private Buffer compressedDirectBuf = null;
    private int uncompressedDirectBufLen;
    private Buffer uncompressedDirectBuf = null;
    private int uncompressedDirectBufOff = 0;
    private byte[] userBuf = null;
    private int userBufOff = 0, userBufLen = 0;
    private boolean finish, finished;
    private boolean keepUncompressedBuf = false;
    private int end_of_stream = 0;

    private long bytesRead = 0L;
    private long bytesWritten = 0L;

    private static CompressionGzipFlag gzipFlag;

    private static boolean nativeIsalLoaded = false;

    public static enum CompressionLevel{
        ISAL_DEF_MIN_LEVEL (0),

        ISAL_DEF_MAX_LEVEL (1);

        private final int compressionLevel;

        CompressionLevel(int level){compressionLevel = level;}

        int compressionLevel(){return compressionLevel;}
    }

    public static enum CompressionGzipFlag{

        IGZIP_DEFLATE(0),

        IGZIP_GZIP(1),

        IGZIP_GZIP_NO_HDR(2);

        private final int gzipflag;

        CompressionGzipFlag(int gzipflag) {this.gzipflag = gzipflag;}

        public int getGzipflag(){return gzipflag;}
    }

    public static enum CompressionFlush{
        NO_FLUSH(0),

        SYNC_FLUSH(1),

        FULL_FLUSH(2);

        private final int compressionFlush;

        CompressionFlush(int flushtype){compressionFlush = flushtype;}

        public int compressionFlush(){return compressionFlush;}

    }

    private static CompressionLevel level;
    private static CompressionFlush flush;

    static {
        if (NativeCodeLoader.isNativeCodeLoaded() &&
                NativeCodeLoader.buildSupportsIsal()) {
            try {
                initIDs();
                nativeIsalLoaded = true;
            } catch (Throwable t) {
                LOG.error("failed to load IsalCompressor", t);
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
    public IsalCompressor( int directBufferSize, CompressionLevel level,CompressionGzipFlag gzipFlag,CompressionFlush flush) {
        this.directBufferSize = directBufferSize;
        this.BufferSize = directBufferSize;
        this.flush = flush;
        this.level = level;
        this.gzipFlag = gzipFlag;
        uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
        compressedDirectBuf = ByteBuffer.allocateDirect(BufferSize);
        compressedDirectBuf.position(BufferSize);
        stream = init(level.compressionLevel(),flush.compressionFlush(),gzipFlag.getGzipflag());
    }

    /**
     * Creates a new compressor with the default buffer size.
     */
    public IsalCompressor() {
        this(DEFAULT_DIRECT_BUFFER_SIZE, CompressionLevel.ISAL_DEF_MAX_LEVEL, CompressionGzipFlag.IGZIP_DEFLATE, CompressionFlush.NO_FLUSH);
    }


    /**
     * Creates a new compressor, taking settings from the configuration.
     */
    public IsalCompressor(Configuration conf) {
        this(IsalFactory.getDirectBufferSize(conf),
             IsalFactory.getCompressionLevel(conf),
             CompressionGzipFlag.IGZIP_DEFLATE,
             CompressionFlush.NO_FLUSH);

    }

    /**
     * Sets input data for compression.
     * This should be called whenever #needsInput() returns
     * <code>true</code> indicating that more input data is required.
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
        uncompressedDirectBufOff = 0;
        setInputFromSavedData();

        // Reinitialize isal's output direct buffer
        compressedDirectBuf.limit(BufferSize);
        compressedDirectBuf.position(BufferSize);

        bytesRead += len;
    }

    /**
     * If a write would exceed the capacity of the direct buffers, it is set
     * aside to be loaded by this function while the compressed data are
     * consumed.
     */
    void setInputFromSavedData() {
        int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
        ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
        userBufLen -= len;
        userBufOff += len;
        uncompressedDirectBufLen = uncompressedDirectBuf.position();

    }

    /**
     * Prepare the compressor to be used in a new stream with settings defined in
     * the given Configuration. It will reset the compressor's compression level
     * and compression strategy.
     *
     * @param conf Configuration storing new settings
     */
    @Override
    public void reinit(Configuration conf) {
        reset();
        if (conf == null) {
            return;
        }
        end(stream);
        level = IsalFactory.getCompressionLevel(conf);
        flush = IsalFactory.getFlush(conf);
        gzipFlag = CompressionGzipFlag.IGZIP_DEFLATE;
        stream = init(level.compressionLevel(),
                flush.compressionFlush(),
                gzipFlag.getGzipflag());
        if(LOG.isDebugEnabled()) {
            LOG.debug("Reinit compressor with new compression configuration");
        }
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
     * #setInput() should be called to provide more input.
     *
     * @return <code>true</code> if the input data buffer is empty and
     *         #setInput() should be called in order to provide more input.
     */
    @Override
    public boolean needsInput() {
        // Consume remaining compressed data?
        if (compressedDirectBuf.remaining() > 0) {
            return false;
        }

        // Check if isal has consumed all input
        // compress should be invoked if keepUncompressedBuf true
        if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
            return false;

        if (uncompressedDirectBuf.remaining() > 0) {
            // Check if we have consumed all user-input
            if (userBufLen <= 0 && uncompressedDirectBufLen <= 0) {
                return true;
            } else {
                // copy enough data from userBuf to uncompressedDirectBuf
                setInputFromSavedData();
                if (uncompressedDirectBuf.remaining() > 0) // uncompressedDirectBuf is not full
                    return true;
                else
                    return false;
            }
        }
       return false;
    }

    /**
     * When called, indicates that compression should end
     * with the current contents of the input buffer.
     */
    @Override
    public void finish() {
        finish = true;
    }

    /**
     * Returns true if the end of the compressed
     * data output stream has been reached.
     *
     * @return <code>true</code> if the end of the compressed
     *         data output stream has been reached.
     */
    @Override
    public boolean finished() {
        // Check if all uncompressed data has been consumed
        return ( finished && compressedDirectBuf.remaining() == 0);
    }

    /**
     * Fills specified buffer with compressed data. Returns actual number
     * of bytes of compressed data. A return value of 0 indicates that
     * needsInput() should be called in order to determine if more input
     * data is required.
     *
     * @param b   Buffer for the compressed data
     * @param off Start offset of the data
     * @param len Size of the buffer
     * @return The actual number of bytes of compressed data.
     */
    @Override
    public int compress(byte[] b, int off, int len)
            throws IOException {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        // Check if there is compressed data
        int n = compressedDirectBuf.remaining();
        if (n > 0) {
            n = Math.min(n, len);
            ((ByteBuffer) compressedDirectBuf).get(b, off, n);
            bytesWritten += n;
            return n;
        }

        // Re-initialize the isal's output direct-buffer
        compressedDirectBuf.rewind();
        compressedDirectBuf.limit(BufferSize); // or directBuffersize?

        if(finish){
            end_of_stream = 1;
        }

        // Compress data
        n = compressBytesDirect();

        //compressedDirectBuf.flip();

        compressedDirectBuf.limit(n);


        // Set 'finished' if isal has consumed all user-data
        if (uncompressedDirectBufLen <= 0) {
            keepUncompressedBuf = false;
            uncompressedDirectBuf.clear();
            uncompressedDirectBufOff = 0;
            uncompressedDirectBufLen = 0;
        }
        else{
            keepUncompressedBuf = true;
        }

        // Get atmost 'len' bytes
        n = Math.min(n, len);
        bytesWritten += n;
        //compressedDirectBuf.rewind();
        ((ByteBuffer) compressedDirectBuf).get(b, off, n);

        return n;
    }

    /**
     * Resets compressor so that a new set of input data can be processed.
     */
    @Override
    public void reset() {
        checkStream();
        reset(stream);
        finish = false;
        finished = false;
        uncompressedDirectBuf.clear();
        uncompressedDirectBufLen = 0;
        uncompressedDirectBufOff = 0;

        compressedDirectBuf.limit(BufferSize);
        compressedDirectBuf.position(BufferSize);
        userBufOff = userBufLen = 0;
        bytesRead = bytesWritten = 0L;

        keepUncompressedBuf = false;

    }

    /**
     * Return number of bytes given to this compressor since last reset.
     */
    @Override
    public long getBytesRead() {
        checkStream();
        return getBytesRead(stream);
    }

    /**
     * Return number of bytes consumed by callers of compress since last reset.
     */
    @Override
    public long getBytesWritten() {
        checkStream();
        return getBytesWritten(stream);
    }

    /**
     * Closes the compressor and discards any unprocessed input.
     */
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

    private native int compressBytesDirect();

    private native static long init(int level,int flush,int gzip_flag);

    private native static long getBytesRead(long strm);

    private native static long getBytesWritten(long strm);

    private native static void end(long strm);

    private native static void reset(long strm);

    public native static String getLibraryName();


}

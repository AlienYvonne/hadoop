/**
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

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressDecompressTester;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.CompressDecompressTester.CompressionTestStrategy;
import org.apache.hadoop.io.compress.isal.IsalCompressor.CompressionLevel;
import org.apache.hadoop.io.compress.isal.IsalDecompressor.IsalDirectDecompressor;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.Before;
import org.junit.Test;
import com.google.common.collect.ImmutableSet;

public class TestIsalCompressorDecompressor {

    private static final Random random = new Random(12345L);

    @Before
    public void before() {
        assumeTrue(IsalFactory.isNativeIsalLoaded(new Configuration()));
    }

    @Test
    public void testIsalCompressorDecompressor() {
        try {
            int SIZE = 44 * 1024;
            byte[] rawData = generate(SIZE);

            CompressDecompressTester.of(rawData)
                    .withCompressDecompressPair(new IsalCompressor(), new IsalDecompressor())
                    .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
                    .test();
        } catch (Exception ex) {
            fail("testCompressorDecompressor error !!!" + ex);
        }
    }

    @Test
    public void testCompressorDecompressorWithExeedBufferLimit() {
        int BYTE_SIZE = 100 * 1024;
        byte[] rawData = generate(BYTE_SIZE);
        try {
            CompressDecompressTester.of(rawData)
                    .withCompressDecompressPair(
                            new IsalCompressor(),
                            new IsalDecompressor())
                    .withTestCases(ImmutableSet.of(CompressionTestStrategy.COMPRESS_DECOMPRESS_SINGLE_BLOCK,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_BLOCK,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_ERRORS,
                            CompressionTestStrategy.COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM))
                    .test();
        } catch (Exception ex) {
            fail("testCompressorDecompressorWithExeedBufferLimit error !!!" + ex);
        }
    }


    @Test
    public void testIsalCompressorDecompressorWithConfiguration() {
        Configuration conf = new Configuration();
        if (IsalFactory.isNativeIsalLoaded(conf)) {
            byte[] rawData;
            int tryNumber = 5;
            int BYTE_SIZE = 10 * 1024;
            Compressor isalCompressor = IsalFactory.getIsalCompressor(conf);
            Decompressor isalDecompressor = IsalFactory.getIsalDecompressor(conf);
            rawData = generate(BYTE_SIZE);
            try {
                for (int i = 0; i < tryNumber; i++)
                    compressDecompressIsal(rawData, (IsalCompressor) isalCompressor,
                            (IsalDecompressor) isalDecompressor);
                isalCompressor.reinit(conf);
            } catch (Exception ex) {
                fail("testIsalCompressorDecompressorWithConfiguration ex error " + ex);
            }
        } else {
            assertTrue("IsalFactory is using native libs against request",
                    IsalFactory.isNativeIsalLoaded(conf));
        }
    }

    @Test
    public void testIsalCompressorDecompressorWithCompressionLevels() {
        Configuration conf = new Configuration();
        conf.set("isal.compress.level","ISAL_DEF_MAX_LEVEL");
        if (IsalFactory.isNativeIsalLoaded(conf)) {
            byte[] rawData;
            int tryNumber = 5;
            int BYTE_SIZE = 10 * 1024;
            Compressor isalCompressor = IsalFactory.getIsalCompressor(conf);
            Decompressor isalDecompressor = IsalFactory.getIsalDecompressor(conf);
            rawData = generate(BYTE_SIZE);
            try {
                for (int i = 0; i < tryNumber; i++)
                    compressDecompressIsal(rawData, (IsalCompressor) isalCompressor,
                            (IsalDecompressor) isalDecompressor);
                isalCompressor.reinit(conf);
            } catch (Exception ex) {
                fail("testIsalCompressorDecompressorWithConfiguration ex error " + ex);
            }
        } else {
            assertTrue("IsalFactory is using native libs against request",
                    IsalFactory.isNativeIsalLoaded(conf));
        }
    }

    @Test
    public void testIsalCompressDecompress() {
        byte[] rawData = null;
        int rawDataSize = 0;
        rawDataSize = 1024 * 64;
        rawData = generate(rawDataSize);
        try {
            IsalCompressor compressor = new IsalCompressor();
            IsalDecompressor decompressor = new IsalDecompressor();
            assertFalse("testIsalCompressDecompress finished error",
                    compressor.finished());
            compressor.setInput(rawData, 0, rawData.length);
            assertTrue("testIsalCompressDecompress getBytesRead before error",
                    compressor.getBytesRead() == 0);
            compressor.finish();

            byte[] compressedResult = new byte[rawDataSize];
            int cSize = compressor.compress(compressedResult, 0, rawDataSize);
            assertTrue("testIsalCompressDecompress getBytesRead ather error",
                    compressor.getBytesRead() == rawDataSize);
            assertTrue(
                    "testIsalCompressDecompress compressed size no less then original size",
                    cSize < rawDataSize);
            decompressor.setInput(compressedResult, 0, cSize);
            byte[] decompressedBytes = new byte[rawDataSize];
            decompressor.decompress(decompressedBytes, 0, decompressedBytes.length);
            assertArrayEquals("testIsalCompressDecompress arrays not equals ",
                    rawData, decompressedBytes);
            compressor.reset();
            decompressor.reset();
        } catch (IOException ex) {
            fail("testIsalCompressDecompress ex !!!" + ex);
        }
    }


    private void compressDecompressLoop(int rawDataSize) throws IOException {
        byte[] rawData = null;
        rawData = generate(rawDataSize);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(rawDataSize);//(rawDataSize+12);
        DeflaterOutputStream dos = new DeflaterOutputStream(baos);
        dos.write(rawData);
        dos.flush();
        dos.close();
        byte[] compressedResult = baos.toByteArray();
        int compressedSize = compressedResult.length;
        IsalDirectDecompressor decompressor = new IsalDirectDecompressor();

        ByteBuffer inBuf = ByteBuffer.allocateDirect(compressedSize);
        ByteBuffer outBuf = ByteBuffer.allocateDirect(rawDataSize);

        inBuf.put(compressedResult, 0, compressedSize);
        inBuf.flip();

        ByteBuffer expected = ByteBuffer.wrap(rawData);

        outBuf.clear();
        while(!decompressor.finished()) {
            decompressor.decompress(inBuf, outBuf);
            if (outBuf.remaining() == 0) {
                outBuf.flip();
                while (outBuf.remaining() > 0) {
                    assertEquals(expected.get(), outBuf.get());
                }
                outBuf.clear();
            }
        }
        outBuf.flip();
        while (outBuf.remaining() > 0) {
            assertEquals(expected.get(), outBuf.get());
        }
        outBuf.clear();

        assertEquals(0, expected.remaining());
    }

    @Test
    public void testIsalDirectCompressDecompress() {
        int[] size = { 1, 4, 16, 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
        assumeTrue(NativeCodeLoader.isNativeCodeLoaded());
        try {
            for (int i = 0; i < size.length; i++) {
                compressDecompressLoop(size[i]);
            }
        } catch (IOException ex) {
            fail("testIsalDirectCompressDecompress ex !!!" + ex);
        }
    }

    @Test
    public void testIsalCompressorDecompressorSetDictionary() {
        Configuration conf = new Configuration();
        if (IsalFactory.isNativeIsalLoaded(conf)) {
            Compressor isalCompressor = IsalFactory.getIsalCompressor(conf);
            Decompressor isalDecompressor = IsalFactory.getIsalDecompressor(conf);

            checkSetDictionaryNullPointerException(isalCompressor);
            checkSetDictionaryNullPointerException(isalDecompressor);

            checkSetDictionaryArrayIndexOutOfBoundsException(isalDecompressor);
            checkSetDictionaryArrayIndexOutOfBoundsException(isalCompressor);
        } else {
            assertTrue("IsalFactory is using native libs against request",
                    IsalFactory.isNativeIsalLoaded(conf));
        }
    }

    @Test
    public void testIsalFactory() {
        Configuration cfg = new Configuration();

        assertTrue("testIsalFactory compression level error !!!",
                CompressionLevel.ISAL_DEF_MAX_LEVEL == IsalFactory
                        .getCompressionLevel(cfg));

        //assertTrue("testIsalFactory compression strategy error !!!",
        // CompressionStrategy.DEFAULT_STRATEGY == IsalFactory
        //                .getCompressionStrategy(cfg));

        IsalFactory.setCompressionLevel(cfg, CompressionLevel.ISAL_DEF_MIN_LEVEL);
        assertTrue("testIsalFactory compression strategy error !!!",
                CompressionLevel.ISAL_DEF_MIN_LEVEL == IsalFactory
                        .getCompressionLevel(cfg));

        //IsalFactory.setCompressionStrategy(cfg, CompressionStrategy.FILTERED);
        //assertTrue("testIsalFactory compression strategy error !!!",
        //        CompressionStrategy.FILTERED == IsalFactory.getCompressionStrategy(cfg));
    }


    private boolean checkSetDictionaryNullPointerException(
            Decompressor decompressor) {
        try {
            decompressor.setDictionary(null, 0, 1);
        } catch (NullPointerException ex) {
            return true;
        } catch (Exception ex) {
        }
        return false;
    }

    private boolean checkSetDictionaryNullPointerException(Compressor compressor) {
        try {
            compressor.setDictionary(null, 0, 1);
        } catch (NullPointerException ex) {
            return true;
        } catch (Exception ex) {
        }
        return false;
    }

    private boolean checkSetDictionaryArrayIndexOutOfBoundsException(
            Compressor compressor) {
        try {
            compressor.setDictionary(new byte[] { (byte) 0 }, 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
            return true;
        } catch (Exception e) {
        }
        return false;
    }

    private boolean checkSetDictionaryArrayIndexOutOfBoundsException(
            Decompressor decompressor) {
        try {
            decompressor.setDictionary(new byte[] { (byte) 0 }, 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
            return true;
        } catch (Exception e) {
        }
        return false;
    }

    private byte[] compressDecompressIsal(byte[] rawData,
                                          IsalCompressor isalCompressor, IsalDecompressor isalDecompressor)
            throws IOException {
        int cSize = 0;
        byte[] compressedByte = new byte[rawData.length];
        byte[] decompressedRawData = new byte[rawData.length];
        isalCompressor.setInput(rawData, 0, rawData.length);
        isalCompressor.finish();
        while (!isalCompressor.finished()) {
            cSize = isalCompressor.compress(compressedByte, 0, compressedByte.length);
        }
        isalCompressor.reset();

        assertTrue(isalDecompressor.getBytesWritten() == 0);
        assertTrue(isalDecompressor.getBytesRead() == 0);
        assertTrue(isalDecompressor.needsInput());
        isalDecompressor.setInput(compressedByte, 0, cSize);
        assertFalse(isalDecompressor.needsInput());
        while (!isalDecompressor.finished()) {
            isalDecompressor.decompress(decompressedRawData, 0,
                    decompressedRawData.length);
        }
        assertTrue(isalDecompressor.getBytesWritten() == rawData.length);
        assertTrue(isalDecompressor.getBytesRead() == cSize);
        isalDecompressor.reset();
        assertTrue(isalDecompressor.getRemaining() == 0);
        assertArrayEquals(
                "testIsalCompressorDecompressorWithConfiguration array equals error",
                rawData, decompressedRawData);

        return decompressedRawData;
    }


    public static byte[]  generate(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)random.nextInt(16);
        return data;
    }

    @Test
    public void testIsalCompressDecompressInMultiThreads() throws Exception {
        MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
        for(int i=0;i<10;i++) {
            ctx.addThread( new MultithreadedTestUtil.TestingThread(ctx) {
                @Override
                public void doWork() throws Exception {
                    testIsalCompressDecompress();
                }
            });
        }
        ctx.startThreads();

        ctx.waitFor(60000);
    }
}

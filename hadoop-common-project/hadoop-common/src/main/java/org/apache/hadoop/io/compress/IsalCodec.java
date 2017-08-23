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

package org.apache.hadoop.io.compress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.isal.IsalCompressor;
import org.apache.hadoop.io.compress.isal.IsalDecompressor;
import org.apache.hadoop.io.compress.isal.IsalDecompressor.IsalDirectDecompressor;
import org.apache.hadoop.io.compress.isal.IsalFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * This class creates isal compressors/decompressors.
 */
public class IsalCodec extends DefaultCodec {
    Configuration conf;
    private static final Log LOG= LogFactory.getLog(IsalCodec.class);

    /**
     * Set the configuration to be used by this object.
     *
     * @param conf the configuration object.
     */
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Return the configuration used by this object.
     *
     * @return the configuration object used by this objec.
     */
    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Are the native isal libraries loaded & initialized?
     */
    public static void checkNativeCodeLoaded() {
        //if (!NativeCodeLoader.isNativeCodeLoaded() ||
             //   !NativeCodeLoader.buildSupportsIsal()) {

            if(!NativeCodeLoader.isNativeCodeLoaded()){
                throw new RuntimeException("1");
            }

            if(!NativeCodeLoader.buildSupportsIsal()){
                throw new RuntimeException("2");
            }

            /*throw new RuntimeException("native isal library not available: " +
                    "this version of libhadoop was built without " +
                    "isal support.");*/
        //}
        if (!IsalCompressor.isNativeIsalLoaded()) {
            throw new RuntimeException("native isal library not available: " +
                    "IsalCompressor has not been loaded.");
        }
        if (!IsalDecompressor.isNativeIsalLoaded()) {
            throw new RuntimeException("native isal library not available: " +
                    "IsalDecompressor has not been loaded.");
        }
    }

    public static boolean isNativeCodeLoaded() {
        return IsalCompressor.isNativeIsalLoaded() &&
                IsalDecompressor.isNativeIsalLoaded();
    }

    public static String getLibraryName() {
        return IsalCompressor.getLibraryName();
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream}.
     *
     * @param out the location for the final output stream
     * @return a stream the user can write uncompressed data to have it compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out)
            throws IOException {
        if (!IsalFactory.isNativeIsalLoaded(conf)) {
            throw new RuntimeException("native isal library not available: " +
                    "IsalDecompressor has not been loaded.");
        }
        return CompressionCodec.Util.
                createOutputStreamWithCodecPool(this, conf, out);
    }

    /**
     * Create a {@link CompressionOutputStream} that will write to the given
     * {@link OutputStream} with the given {@link Compressor}.
     *
     * @param out        the location for the final output stream
     * @param compressor compressor to use
     * @return a stream the user can write uncompressed data to have it compressed
     * @throws IOException
     */
    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor)
            throws IOException {
        return (compressor != null) ?
                new CompressorStream(out, compressor,
                        conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
                                IO_FILE_BUFFER_SIZE_DEFAULT)) :
                createOutputStream(out);
    }

    /**
     * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of compressor needed by this codec.
     */
    @Override
    public Class<? extends Compressor> getCompressorType() {
        checkNativeCodeLoaded();
        return IsalCompressor.class;
    }

    /**
     * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
     *
     * @return a new compressor for use by this codec
     */
    @Override
    public Compressor createCompressor() {
        checkNativeCodeLoaded();
        return new IsalCompressor(conf);
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given
     * input stream.
     *
     * @param in the stream to read compressed bytes from
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    @Override
    public CompressionInputStream createInputStream(InputStream in)
            throws IOException {
        return CompressionCodec.Util.
                createInputStreamWithCodecPool(this, conf, in);
    }

    /**
     * Create a {@link CompressionInputStream} that will read from the given
     * {@link InputStream} with the given {@link Decompressor}.
     *
     * @param in           the stream to read compressed bytes from
     * @param decompressor decompressor to use
     * @return a stream to read uncompressed bytes from
     * @throws IOException
     */
    @Override
    public CompressionInputStream createInputStream(InputStream in,
                                                    Decompressor decompressor)
            throws IOException {
        checkNativeCodeLoaded();
        if (decompressor == null) {
            decompressor = createDecompressor();  // always succeeds (or throws)
        }
        return new DecompressorStream(in, decompressor,
                conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
                        IO_FILE_BUFFER_SIZE_DEFAULT));
    }

    /**
     * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
     *
     * @return the type of decompressor needed by this codec.
     */
    @Override
    public Class<? extends Decompressor> getDecompressorType() {
        checkNativeCodeLoaded();
        return IsalDecompressor.class;
    }

    /**
     * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
     *
     * @return a new decompressor for use by this codec
     */
    @Override
    public Decompressor createDecompressor() {
        checkNativeCodeLoaded();
        return new IsalDecompressor(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DirectDecompressor createDirectDecompressor() {
        return isNativeCodeLoaded() ? new IsalDirectDecompressor() : null;
    }

    /**
     * Get the default filename extension for this kind of compression.
     *
     * @return <code>.isal</code>.
     */
    @Override
    public String getDefaultExtension() {
        return ".isal";
    }
}
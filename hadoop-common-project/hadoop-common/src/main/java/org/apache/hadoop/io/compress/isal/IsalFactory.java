package org.apache.hadoop.io.compress.isal;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.hadoop.io.compress.isal.IsalCompressor.CompressionFlush;
import org.apache.hadoop.io.compress.isal.IsalCompressor.CompressionLevel;
import org.apache.hadoop.util.NativeCodeLoader;

public class IsalFactory {
    private static final Log LOG =
            LogFactory.getLog(IsalFactory.class);

    private static boolean nativeIsalLoaded = false;

    static{
        loadNativeIsal();
    }
    /**
     * Load native library and set the flag whether to use native library. The
     * method is also used for reset the flag modified by setNativeIsalLoaded
     */
    @VisibleForTesting
    public static void loadNativeIsal() {
        if (NativeCodeLoader.isNativeCodeLoaded()) {
            nativeIsalLoaded = IsalCompressor.isNativeIsalLoaded() &&
                    IsalDecompressor.isNativeIsalLoaded();

            if (nativeIsalLoaded) {
                LOG.info("Successfully loaded & initialized native-isal library");
            } else {
                LOG.warn("Failed to load/initialize native-isal library");
            }
        }
    }

    /**
     * Set the flag whether to use native library. Used for testing non-native
     * libraries
     *
     */
    @VisibleForTesting
    public static void setNativeIsalLoaded(final boolean isLoaded) {
        IsalFactory.nativeIsalLoaded = isLoaded;
    }
    /**
     * Check if native-isal code is loaded & initialized correctly and
     * can be loaded for this job.
     *
     * @param conf configuration
     * @return <code>true</code> if native-isal is loaded & initialized
     *         and can be loaded for this job, else <code>false</code>
     */
    public static boolean isNativeIsalLoaded(Configuration conf) {
        return nativeIsalLoaded;
    }

    public static String getLibraryName() {
        return IsalCompressor.getLibraryName();
    }

    /**
     * Return the appropriate type of the isal compressor.
     *
     * @param conf configuration
     * @return the appropriate type of the isal compressor.
     */
    public static Class<? extends Compressor>
    getIsalCompressorType(Configuration conf) {
        return IsalCompressor.class;
    }

    /**
     * Return the appropriate implementation of the isal compressor.
     *
     * @param conf configuration
     * @return the appropriate implementation of the isal compressor.
     */
    public static Compressor getIsalCompressor(Configuration conf) {
        return new IsalCompressor(conf);
                }

    /**
     * Return the appropriate type of the isal decompressor.
     *
     * @param conf configuration
     * @return the appropriate type of the isal decompressor.
     */
    public static Class<? extends Decompressor>
    getIsalDecompressorType(Configuration conf) {
        return IsalDecompressor.class;
    }

    /**
     * Return the appropriate implementation of the isal decompressor.
     *
     * @param conf configuration
     * @return the appropriate implementation of the isal decompressor.
     */
    public static Decompressor getIsalDecompressor(Configuration conf) {
        return new IsalDecompressor();
    }

    /**
     * Return the appropriate implementation of the isal direct decompressor.
     *
     * @param conf configuration
     * @return the appropriate implementation of the isal decompressor.
     */
    public static DirectDecompressor getIsalDirectDecompressor(Configuration conf) {
        return (isNativeIsalLoaded(conf)) ?
                new IsalDecompressor.IsalDirectDecompressor() : null;
    }


    public static void setCompressionLevel(Configuration conf,
                                           CompressionLevel level) {
        conf.setEnum("isal.compress.level", level);
    }

    public static CompressionLevel getCompressionLevel(Configuration conf) {
        return conf.getEnum("isal.compress.level",
                CompressionLevel.ISAL_DEF_MAX_LEVEL);
    }
    public static int getDirectBufferSize(Configuration conf){
        return conf.getInt("isal.compress.directbuffersize",
                64*1024);
    }


    public static CompressionFlush getFlush(Configuration conf){
        return conf.getEnum("isal.compress.flush",
                CompressionFlush.NO_FLUSH);
    }

}

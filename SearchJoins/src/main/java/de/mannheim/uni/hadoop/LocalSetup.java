package de.mannheim.uni.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class LocalSetup {
    private FileSystem fileSystem;
    private Configuration config;

    /** Sets up Configuration and LocalFileSystem instances for
     * Hadoop.  Throws Exception if they fail.  Does not load any
     * Hadoop XML configuration files, just sets the minimum
     * configuration necessary to use the local file system.
     */
    public LocalSetup() throws Exception {
        config = new Configuration();

        /* Normally set in hadoop-default.xml, without it you get
         * "java.io.IOException: No FileSystem for scheme: file" */
        config.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        //config.set("fs.file.impl", "org.apache.hadoop.dfs.HftpFileSystem");
        //org.apache.hadoop.dfs.HftpFileSystem

        fileSystem = FileSystem.get(config);
        if (fileSystem.getConf() == null) {
            /* This happens if the FileSystem is not properly
             * initialized, causes NullPointerException later. */
            throw new Exception("LocalFileSystem configuration is null");
        }
    }

    /** Returns a Hadoop Configuration instance for use in Hadoop API
     * calls. */
    public Configuration getConf() {
        return config;
    }

    /** Returns a Hadoop FileSystem instance that provides access to
     * the local filesystem. */
    public FileSystem getLocalFileSystem() {
        return fileSystem;
    }
}

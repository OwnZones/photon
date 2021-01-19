/*
 *
 * Copyright 2015 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package com.netflix.imflibrary.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * This interface is the supertype for classes representing an file locator across different storage facilities.
 */
public interface FileLocator
{
    public char separator = '/';

    public static FileLocator fromLocation(String location) {
        if (location.startsWith("s3://")) {
            return new S3FileLocator(location);
        }

        return new LocalFileLocator(location);
    }

    public static FileLocator fromLocation(FileLocator directoryLocator, String fileName) throws IOException {
        String directoryPath = directoryLocator.getAbsolutePath();
        if (directoryPath.charAt(directoryPath.length() - 1) != '/') {
            directoryPath += '/';
        }

        return FileLocator.fromLocation(directoryPath + fileName);
    }

    public static FileLocator fromLocation(URI location) {
        if (location.toString().startsWith("s3://")) {
            return new S3FileLocator(location);
        }

        return new LocalFileLocator(location);
    }

    /**
     * Copies all bytes from a file denoted by a file locator to a target file
     */
    public static Path copy(FileLocator fileLocator, Path outputFilePath) throws IOException {
        InputStream inputStream = fileLocator.getInputStream();

        Files.copy(
                inputStream,
                outputFilePath,
                StandardCopyOption.REPLACE_EXISTING);

        inputStream.close();

        return outputFilePath;
    }

    public static File toTmpFile(FileLocator fileLocator) throws IOException {
        File file = File.createTempFile("tmp", "");
        file.deleteOnExit();
        FileLocator.copy(fileLocator, file.toPath());
        return file;
    }

    /**
     * Tests whether the file locator represents a directory.
     */
    public boolean isDirectory() throws IOException;

    /**
     * Return the absolute pathname string denoting the same file or
     * directory as this abstract pathname
     */
    public String getAbsolutePath() throws IOException;

    /**
     * Returns an array of abstract pathnames denoting the files in the
     * directory denoted by this abstract pathname.
     */
    public FileLocator[] listFiles(FilenameFilter filenameFilter) throws IOException;

    /**
     * Constructs a <tt>file:</tt> URI that represents this abstract pathname.
     */
    public URI toURI() throws IOException;

    /**
     * Tests whether the file or directory denoted by this abstract pathname
     * exists.
     */
    public boolean exists() throws IOException;

    /**
     * Returns the name of the file or directory denoted by this abstract
     * pathname.  This is just the last name in the pathname's name
     * sequence.  If the pathname's name sequence is empty, then the empty
     * string is returned.
     */
    public String getName() throws IOException;

    /**
     * Converts this abstract pathname into a pathname string.
     */
    public String getPath() throws IOException;

    /**
     * Returns the length of the file denoted by this abstract pathname.
     * The return value is unspecified if this pathname denotes a directory.
     */
    public long length() throws IOException;

    /**
     *
     */
    public InputStream getInputStream() throws IOException;

    /**
     *
     */
    public ResourceByteRangeProvider getResourceByteRangeProvider();
}

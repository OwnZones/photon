/*
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
 */

package com.netflix.imflibrary.utils;

import testUtils.TestHelper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

@Test(groups = "unit")
public class ByteArrayDataProviderTest
{
    FileLocator inputFile;
    InputStream inputStream;

    @BeforeClass
    public void beforeClass()
    {
        inputFile = TestHelper.findResourceByPath("PKL_e788efe2-1782-4b09-b56d-1336da2413d5.xml");
    }

    @BeforeMethod
    public void beforeMethod() throws IOException
    {
        inputStream = inputFile.getInputStream();
    }

    @AfterMethod
    public void AfterMethod() throws IOException
    {
        if (inputStream != null)
        {
            inputStream.close();
        }
    }

    @Test
    public void testGetBytes() throws IOException
    {
        byte[] refBytes = Arrays.copyOf(TestHelper.toByteArray(inputStream), 100);

        ByteProvider byteProvider = new ByteArrayDataProvider(Files.readAllBytes(Paths.get(this.inputFile.toURI())));
        byte[] bytes = byteProvider.getBytes(100);

        Assert.assertEquals(refBytes, bytes);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Cannot read .*")
    public void testGetBytesLarge() throws IOException
    {
        long length = inputFile.length();
        Assert.assertTrue(length < Integer.MAX_VALUE);

        ByteProvider byteProvider = new ByteArrayDataProvider(Files.readAllBytes(Paths.get(this.inputFile.toURI())));
        byteProvider.getBytes((int)length + 1);
    }

    @Test
    public void testSkipBytes() throws IOException
    {
        ByteProvider byteProvider = new ByteArrayDataProvider(Files.readAllBytes(Paths.get(this.inputFile.toURI())));
        byteProvider.skipBytes(100L);
        byte[] bytes = byteProvider.getBytes(1);
        Assert.assertEquals(bytes.length, 1);
        Assert.assertEquals(bytes[0], 99);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Cannot skip .*")
    public void testSkipBytesLarge() throws IOException
    {
        long length = inputFile.length();
        Assert.assertTrue(length < Integer.MAX_VALUE);

        ByteProvider byteProvider = new ByteArrayDataProvider(Files.readAllBytes(Paths.get(this.inputFile.toURI())));
        byteProvider.skipBytes(length + 1);
    }
}

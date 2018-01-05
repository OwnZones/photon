package com.netflix.imflibrary.utils;
import com.amazonaws.services.s3.AmazonS3URI;

import org.testng.annotations.Test;
import org.testng.Assert;

@Test(groups = "unit")
public class S3ByteRangeProviderTest
{
    private final String S3Url = "s3://ownzones-deploy/zypline/test/SampleVideo_360x240_1mb.mp4";

    @Test
    public void testFileFoundAndLengthValid()
    {
        AmazonS3URI uri = new AmazonS3URI(this.S3Url);
        S3ByteRangeProvider provider = new S3ByteRangeProvider(uri);

        long fileSize = provider.getResourceSize();
        Assert.assertEquals(fileSize, 1053651);
    }
}

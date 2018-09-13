package com.netflix.imflibrary.utils;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class S3FileLocator implements FileLocator {
    private static final AmazonS3 s3Client = new AmazonS3Client(DefaultAWSCredentialsProviderChain.getInstance());

    private String bucket;
    private String key;
    private AmazonS3URI s3URI;
    private long length = 0;

    public S3FileLocator(String url) {
        this.s3URI = new AmazonS3URI(url, true);
        this.bucket = this.s3URI.getBucket();
        this.key = this.s3URI.getKey();
        this.setAWSEndpoint();
    }

    public S3FileLocator(URI url) {
        this.s3URI = new AmazonS3URI(url);
        this.bucket = this.s3URI.getBucket();
        this.key = this.s3URI.getKey();
        this.setAWSEndpoint();
    }

    public S3FileLocator(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        this.s3URI = new AmazonS3URI("s3://" + this.bucket + "/" + this.key, true);
        this.setAWSEndpoint();
    }

    private void setAWSEndpoint() {
        String awsEndpoint =  System.getenv("AWS_S3_ENDPOINT");
        if (awsEndpoint != null && awsEndpoint.length() > 0) {
            s3Client.setEndpoint(awsEndpoint);
            s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
        }
    }

    public static void main(String args[]) throws IOException {
        S3FileLocator fileLocator = new S3FileLocator("s3://oz-zl-api-staging-dev/mao/deliverables/chloes-bag-imf-package-mao-base/");

        System.out.println(fileLocator.isDirectory());

        for (S3FileLocator fl : fileLocator.listFiles(null)) {
            System.out.println(fl.toURI());
        }
    }

    public URI toURI() {
       return this.s3URI.getURI();
    }

    public String getAbsolutePath() {
        return "s3://" + this.bucket + "/" + this.key;
    }

    public String getPath() {
        return this.getAbsolutePath();
    }

    public long length() {
        if (this.length != 0) {
            return this.length;
        }

        ObjectMetadata metadata = s3Client.getObjectMetadata(this.bucket, this.key);
        this.length = metadata.getContentLength();
        return this.length;
    }

    public InputStream getInputStream() throws IOException {
        S3Object s3Object = s3Client.getObject(this.bucket, this.key);
        return s3Object.getObjectContent();
    }

    public ResourceByteRangeProvider getResourceByteRangeProvider() {
        return new S3ByteRangeProvider(this);
    }

    public String getName() {
        String[] parts = this.key.split("/");
        return parts[parts.length - 1];
    }

    public boolean exists() {
        return true;
    }

    /**
     * Tests whether the file denoted by this abstract pathname is a
     * directory.
     *
     * @return <code>true</code> if and only if the file denoted by this
     * abstract pathname exists <em>and</em> is a directory;
     * <code>false</code> otherwise
     */
    @Override
    public boolean isDirectory() {
        return this.key.charAt(this.key.length() - 1) == '/';
    }

    /**
     * Returns the top level keys in a s3 folder
     * @return
     */
    public S3FileLocator[] listFiles(FilenameFilter filenameFilter) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(this.bucket)
                .withPrefix(this.key)
                .withDelimiter("/");

        ListObjectsV2Result result = s3Client.listObjectsV2(req);
        ArrayList<S3FileLocator> fileLocators = new ArrayList<S3FileLocator>();
        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
            S3FileLocator fl = new S3FileLocator(objectSummary.getBucketName(), objectSummary.getKey());
            if (filenameFilter == null || filenameFilter.accept(null, fl.getName())) {
                fileLocators.add(fl);
            }
        }

        return fileLocators.toArray(new S3FileLocator[0]);
    }


}

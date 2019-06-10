package com.ap3x.firewood.services.other;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@Service
public class S3Service {

    public List<String> readFromS3ToStringArray(final String bucket, final String filePath){
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        String object = s3.getObjectAsString(bucket, filePath);
        return Arrays.asList(object.split("\n"));
    }

    public String downloadFromS3(final String bucket, final String key) {
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        String localPath = "/tmp/faio.properties";
        File localFile = new File(localPath);
        ObjectMetadata object = s3.getObject(new GetObjectRequest(bucket, key), localFile);

        if (localFile.exists() && localFile.canRead())
            return localPath;
        throw new RuntimeException("Could not save file from S3");
    }
}

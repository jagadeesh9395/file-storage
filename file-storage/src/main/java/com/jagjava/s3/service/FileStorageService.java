package com.jagjava.s3.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

@Service
public class FileStorageService {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(FileStorageService.class);

    private AmazonS3 s3;

    public FileStorageService(AmazonS3 s3) {
        this.s3 = s3;
    }

    public PutObjectResult upload(
            String path,
            String fileName,
            Optional<Map<String, String>> optionalMetaData,
            InputStream inputStream) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        optionalMetaData.ifPresent(map -> {
            if (!map.isEmpty()) {
                map.forEach(objectMetadata::addUserMetadata);
            }
        });
        log.debug("Path: {} , FileName : {} ", path, fileName);
        return s3.putObject(path, fileName, inputStream, objectMetadata);
    }

}

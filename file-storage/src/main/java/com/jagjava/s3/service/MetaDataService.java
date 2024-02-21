package com.jagjava.s3.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class MetaDataService {
    private FileStorageService fileStorageService;

    @Value("${aws.s3.bucket}")
    private String bucketName;


    public MetaDataService(FileStorageService fileStorageService) {
        this.fileStorageService = fileStorageService;
    }

    public void upload(MultipartFile file) throws IOException {

        if (file.isEmpty())
            throw new IllegalStateException("Cannot upload empty file");

        Map<String, String> metadata = new HashMap<>();
        metadata.put("Content-Type", file.getContentType());
        metadata.put("Content-Length", String.valueOf(file.getSize()));

        String fileName = String.format("%s", file.getOriginalFilename());

        // Uploading file to s3
        fileStorageService.upload(bucketName, fileName, Optional.of(metadata), file.getInputStream());
    }
}

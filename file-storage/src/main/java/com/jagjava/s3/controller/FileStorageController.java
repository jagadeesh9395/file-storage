package com.jagjava.s3.controller;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.jagjava.s3.repository.CustomerRepository;
import com.jagjava.s3.service.MetaDataService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

@RestController
@RequestMapping("/file-storage")
public class FileStorageController {
    MetaDataService metaDataService;

    AmazonS3 s3Client;
    CustomerRepository customerRepository;

    public FileStorageController(MetaDataService metaDataService, AmazonS3 s3Client, CustomerRepository customerRepository) {
        this.metaDataService = metaDataService;
        this.s3Client = s3Client;
        this.customerRepository = customerRepository;
    }

    @PostMapping("/upload")
    public String upload(@RequestParam("file") MultipartFile file) throws IOException {
        metaDataService.upload(file);
        return "Upload Successfully";
    }


    @PostMapping("/directSaveCsv")
    public  String directSaveCsv() throws IOException {
        ObjectMetadata objectMetadata=new ObjectMetadata();
        objectMetadata.setContentType("text/csv");
         s3Client.putObject("jagstorage", "customertyu.csv", new ByteArrayInputStream(customerRepository.findAll().toString().getBytes(StandardCharsets.UTF_8)), objectMetadata);

          return "Upload Successfully CSV";
    }


}

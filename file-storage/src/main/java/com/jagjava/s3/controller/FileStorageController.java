package com.jagjava.s3.controller;

import com.jagjava.s3.service.MetaDataService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/file-storage")
public class FileStorageController {
    MetaDataService metaDataService;

    public FileStorageController(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    @PostMapping("/upload")
    public String upload(@RequestParam("file") MultipartFile file) throws IOException {
        metaDataService.upload(file);
        return "Upload Successfully";
    }

}

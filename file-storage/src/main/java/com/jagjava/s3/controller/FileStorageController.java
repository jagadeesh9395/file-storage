package com.jagjava.s3.controller;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.jagjava.s3.entity.MailStructure;
import com.jagjava.s3.repository.CustomerRepository;
import com.jagjava.s3.service.FileStorageService;
import com.jagjava.s3.service.MetaDataService;
import com.opencsv.CSVWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;

@RestController
@RequestMapping("/file-storage")
public class FileStorageController {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(FileStorageController.class);
    @Value("${spring.datasource.url}")
    String dbUrl;

    @Value("${spring.datasource.username}")
    String dbUser;
    @Value("${spring.datasource.password}")
    String dbPass;

    @Value("${aws.s3.bucket}")
    String awsBucketName;
    MetaDataService metaDataService;

    AmazonS3 s3Client;
    CustomerRepository customerRepository;

    FileStorageService fileStorageService;

    public FileStorageController(MetaDataService metaDataService, AmazonS3 s3Client, CustomerRepository customerRepository, FileStorageService fileStorageService) {
        this.metaDataService = metaDataService;
        this.s3Client = s3Client;
        this.customerRepository = customerRepository;
        this.fileStorageService = fileStorageService;
    }

    @PostMapping("/upload")
    public String upload(@RequestParam("file") MultipartFile file) throws IOException {
        metaDataService.upload(file);
        return "Upload Successfully";
    }


    @PostMapping("/directSaveCsv")
    public String directSaveCsv(@RequestBody MailStructure mailStructure) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
        try (CSVWriter writer = buildCSVWriter(streamWriter)) {
            Connection con = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            log.info("Connection established......");
            //Creating the Statement
            Statement stmt = con.createStatement();
            //Query to retrieve records
            String query = "Select * from customers_info";
            //Executing the query
            stmt.executeQuery(query);
            ResultSet rs = stmt.executeQuery(query);
            //Instantiating the CSVWriter class
            ResultSetMetaData rsMetaData = rs.getMetaData();
            rsMetaData.getColumnName(1);
            //Writing data to a csv file
            String[] line1 = {rsMetaData.getColumnName(1), rsMetaData.getColumnName(2), rsMetaData.getColumnName(3), rsMetaData.getColumnName(4), rsMetaData.getColumnName(5), rsMetaData.getColumnName(6), rsMetaData.getColumnName(7), rsMetaData.getColumnName(8)};
            writer.writeNext(line1);
            String[] data = new String[8];
            while (rs.next()) {
                data[0] = String.valueOf(rs.getInt("customer_id"));
                data[1] = rs.getString("contact");
                data[2] = rs.getString("country");
                data[3] = rs.getString("dob");
                data[4] = rs.getString("email");
                data[5] = rs.getString("first_name");
                data[6] = rs.getString("gender");
                data[7] = rs.getString("last_name");
                writer.writeNext(data);
            }
            //Flushing data from writer to file
            writer.flush();
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentType("text/csv");
            String filename = "cust - " + RandomStringUtils.randomAlphabetic(5).toLowerCase() + ".csv";
            s3Client.putObject(awsBucketName, filename, new ByteArrayInputStream(stream.toByteArray()), meta);
            java.util.Date expiration = new java.util.Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis += 1000 * 60 * 60;
            expiration.setTime(expTimeMillis);
            URL url1 = s3Client.generatePresignedUrl(awsBucketName, filename, expiration, HttpMethod.GET);
            mailStructure.setSubject("Download CSV file from below link");
            mailStructure.setBody(String.valueOf(url1));
            fileStorageService.sendMail(mailStructure.getEmail(), mailStructure);
            return "Successfully saved into s3 and sent to email " + mailStructure.getEmail();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private CSVWriter buildCSVWriter(OutputStreamWriter streamWriter) {
        return new CSVWriter(streamWriter, ',', Character.MIN_VALUE, '"', System.lineSeparator());
    }
}
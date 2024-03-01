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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.Date;

@RestController
@RequestMapping("/file-storage")
public class FileStorageController {
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
    public String directSaveCsv() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
        try (CSVWriter writer = buildCSVWriter(streamWriter)) {
            String url = "jdbc:mysql://localhost:3306/batchjob";
            Connection con = DriverManager.getConnection(url, "root", "Password");
            System.out.println("Connection established......");
            //Creating the Statement
            Statement stmt = con.createStatement();
            //Query to retrieve records
            String query = "Select * from customers_info";
            //Executing the query
            stmt.executeQuery(query);
            ResultSet rs = stmt.executeQuery(query);
            //Instantiating the CSVWriter class
            ResultSetMetaData Mdata = rs.getMetaData();
            Mdata.getColumnName(1);
            //Writing data to a csv file
            String line1[] = {Mdata.getColumnName(1), Mdata.getColumnName(2), Mdata.getColumnName(3), Mdata.getColumnName(4), Mdata.getColumnName(5), Mdata.getColumnName(6), Mdata.getColumnName(7), Mdata.getColumnName(8)};
            writer.writeNext(line1);
            String data[] = new String[8];
            while (rs.next()) {
                data[0] = String.valueOf(rs.getInt("CUSTOMER_ID"));
                data[1] = rs.getString("FIRST_NAME");
                data[2] = rs.getString("LAST_NAME");
                data[3] = rs.getString("EMAIL");
                data[4] = rs.getString("GENDER");
                data[5] = rs.getString("CONTACT");
                data[6] = rs.getString("COUNTRY");
                data[7] = rs.getString("DOB");

                writer.writeNext(data);
            }
            //Flushing data from writer to file
            writer.flush();
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentType("text/csv");
            s3Client.putObject("mybuckettopractice", "cust - " + RandomStringUtils.randomAlphabetic(5) + ".csv", new ByteArrayInputStream(stream.toByteArray()), meta);
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.HOUR, 10);
            calendar.setTime(new Date());

            URL url1 = s3Client.generatePresignedUrl("s3storagerajesh", "cust - " + RandomStringUtils.randomAlphabetic(5).toLowerCase() + ".csv", calendar.getTime(), HttpMethod.GET);

            System.out.println("generated URL " + url1.toString());
            return "Successfully saved";
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private CSVWriter buildCSVWriter(OutputStreamWriter streamWriter) {
        return new CSVWriter(streamWriter, ',', Character.MIN_VALUE, '"', System.lineSeparator());
    }

    @PostMapping(value = "/sendMail/{toEmail}")
    public String sendMail(@PathVariable String toEmail, @RequestBody MailStructure mailStructure) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, 10);
        calendar.setTime(new Date());

        URL url1 = s3Client.generatePresignedUrl("s3storagerajesh", "cust - " + RandomStringUtils.randomAlphabetic(5).toLowerCase() + ".csv", calendar.getTime(), HttpMethod.GET);
        mailStructure.setSubject("Download CSV file from below link");
        mailStructure.setBody(String.valueOf(url1));
        fileStorageService.sendMail(toEmail, mailStructure);
        return "Mail Sent";
    }
}




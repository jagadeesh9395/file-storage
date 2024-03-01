package com.jagjava.s3.service;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.jagjava.s3.entity.MailStructure;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;

@Service
public class FileStorageService {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(FileStorageService.class);


    @Value("${spring.mail.username}")
    private String fromEmail;
    private AmazonS3 s3;

    private JavaMailSender mailSender;

    public FileStorageService(AmazonS3 s3, JavaMailSender mailSender) {
        this.s3 = s3;
        this.mailSender = mailSender;
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

    public void sendMail(String toMail, MailStructure mailStructure) {

        SimpleMailMessage mailMessage = new SimpleMailMessage();
        mailMessage.setFrom(fromEmail);
        mailMessage.setSubject(mailStructure.getSubject());
        mailMessage.setText(mailStructure.getBody());
        mailMessage.setTo(toMail);
        mailSender.send(mailMessage);
        log.info(MessageFormat.format("Sending mail to {0} with message {1}", toMail, mailStructure.getBody()));
    }

}

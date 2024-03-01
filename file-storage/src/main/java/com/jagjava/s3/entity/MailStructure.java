package com.jagjava.s3.entity;


import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MailStructure {
    private String subject;
    private String body;
}

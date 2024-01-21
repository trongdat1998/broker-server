package io.bhex.broker.server.grpc.server.service.po;

import lombok.Data;

import java.util.List;

@Data
public class ComplianceDictionary {

    private List<String> countries;
    private List<String> industries;
    private List<String> professions;
    private List<String> cardTypes;
    private List<String> incomeSources;
    private List<String> incomeRange;
    private List<String> salutations;
    private List<String> entityTypes;
    private List<String> addressProofTypes;
    private List<String> workProofTypes;
    private List<String> residentStatus;
    private List<String> depositWithdrawAmounts;
    private List<String> employmentStatus;
    private List<String> ownershipLayers;
}


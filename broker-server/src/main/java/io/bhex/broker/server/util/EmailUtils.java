package io.bhex.broker.server.util;

public class EmailUtils {

    public static String emailAlias(String email) {
        if (email.contains("@")) {
            String emailPrefix = email.substring(0, email.indexOf("@"));
            String emailSuffix = email.substring(email.indexOf("@"));
            if (emailSuffix.equalsIgnoreCase("@gmail.com")) {
                String emailName = emailPrefix.replaceAll("\\.", "");
                if (emailName.contains("+")) {
                    emailName = emailName.substring(0, emailName.indexOf("+"));
                }
                return emailName + emailSuffix;
            } else if (emailSuffix.endsWith("@hotmail.com") || emailSuffix.endsWith("@outlook.com")) {
                String emailName = emailPrefix;
                if (emailName.contains("+")) {
                    emailName = emailName.substring(0, emailName.indexOf("+"));
                }
                return emailName + emailSuffix;
            }
        }
        return email;
    }

}

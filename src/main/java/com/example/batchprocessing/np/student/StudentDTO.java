package com.example.batchprocessing.np.student;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains the information of a single student who has purchased
 * the course.
 */
@XmlRootElement(name = "student")
public class StudentDTO {

    private String emailAddress;
    private String name;
    private String purchasedPackage;

    public StudentDTO() {
    }

    public String getEmailAddress() {
        return emailAddress;
    }

    public String getName() {
        return name;
    }

    public String getPurchasedPackage() {
        return purchasedPackage;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPurchasedPackage(String purchasedPackage) {
        this.purchasedPackage = purchasedPackage;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("emailAddress " + this.emailAddress)
                .append("name " + this.name)
                .append("purchasedPackage " + this.purchasedPackage)
                .toString();
    }
}

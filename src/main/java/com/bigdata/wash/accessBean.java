package com.bigdata.wash;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class accessBean implements Writable {
    private String timeStamp;
    private String IPAddress;
    private String cookie;
    private String session;
    private String requestURL;
    private String referal;

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    private boolean valid;
    private String status;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }


    @Override
    public String toString() {
        return timeStamp + '\t' + IPAddress + '\t' + cookie + '\t' + session + '\t' + requestURL + '\t' + referal;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getIPAddress() {
        return IPAddress;
    }

    public void setIPAddress(String IPAddress) {
        this.IPAddress = IPAddress;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    public String getReferal() {
        return referal;
    }

    public void setReferal(String referal) {
        this.referal = referal;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(timeStamp);
        dataOutput.writeBytes(IPAddress);
        dataOutput.writeBytes(cookie);
        dataOutput.writeBytes(session);
        dataOutput.writeBytes(requestURL);
        dataOutput.writeBytes(referal);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.timeStamp = dataInput.readUTF();
        this.IPAddress = dataInput.readUTF();
        this.cookie = dataInput.readUTF();
        this.session = dataInput.readUTF();
        this.requestURL = dataInput.readUTF();
        this.referal = dataInput.readUTF();
    }
}

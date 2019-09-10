package com.lzx.LogClean1;

public class LogBean {
    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public boolean isVail() {
        return vail;
    }

    public void setVail(boolean vail) {
        this.vail = vail;
    }


    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(this.addr);
        s.append("\001").append(this.user);
        s.append("\001").append(this.time);
        s.append("\001").append(this.request);
        s.append("\001").append(this.status);
        s.append("\001").append(this.size);
        s.append("\001").append(this.referer);
        s.append("\001").append(this.user_agent);
        return s.toString();
    }

    //store ip
    private String addr;
    //client user name
    private String user;
    //visit time
    private String time;
    //user url and http protocol type
    private String request;
    //state type;
    private String status;
    //size of file sent to client
    private String size;
    //record where you are
    private String referer;
    //record info of browser from client
    private String user_agent;
    //judge wether it is legal
    private boolean vail=true;

    //

}

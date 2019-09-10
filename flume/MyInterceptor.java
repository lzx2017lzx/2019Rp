package flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class MyInterceptor implements Interceptor {
    public void initialize() {

    }

    /*
    intercept and get events

     */
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        //transfer to upper
        event.setBody(new String(body).toUpperCase().getBytes());
        return event;
    }

    /*
    get filter event gather
     */
    public List<Event> intercept(List<Event> list) {
        List<Event> list1 = new ArrayList<Event>();

        for(Event e: list){
            list1.add(intercept(e));
        }
        return list1;
    }

    public void close() {

    }

    public static class Build implements Interceptor.Builder{
        public void configure(Context context) {

        }

        //get self define interceptor
        public Interceptor build() {
            return new MyInterceptor();
        }
    }
}

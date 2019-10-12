package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2019/1/18 0018.
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        String body = new String(event.getBody(), Charset.forName("UTF-8"));

        // body为原始数据，newBody为处理后的数据,判断是否为display的数据类型
        if (LogUtils.validateReportLog(body)) {
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> intercepted = new ArrayList<>(events.size());

        for (Event event : events) {
            Event interceptedEvent = intercept(event);

            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }

        return intercepted;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new LogETLInterceptor();
        }


        @Override
        public void configure(Context context) {

        }
    }
}

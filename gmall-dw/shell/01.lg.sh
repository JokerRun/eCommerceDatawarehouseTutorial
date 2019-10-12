#! /bin/bash

    for i in machine001 machine002 
    do
        ssh $i "source /etc/profile && java -classpath /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain >/opt/module/test.log &"
    done

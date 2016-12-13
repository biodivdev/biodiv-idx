FROM diogok/java8:zulu

WORKDIR /opt
CMD ["java","-server","-XX:+UseConcMarkSweepGC","-XX:+UseCompressedOops","-XX:+DoEscapeAnalysis","-jar","rrapp-idx.jar"]

ADD target/rrapp-idx-0.0.4-standalone.jar /opt/rrapp-idx.jar


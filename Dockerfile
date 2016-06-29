FROM diogok/java8:zulu

WORKDIR /opt
CMD ["java","-server","-XX:+UseConcMarkSweepGC","-XX:+UseCompressedOops","-XX:+DoEscapeAnalysis","-jar","biodividx.jar"]

ADD target/biodividx-0.0.1-standalone.jar /opt/biodividx.jar


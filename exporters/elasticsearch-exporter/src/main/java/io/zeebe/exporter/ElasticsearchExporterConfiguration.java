package io.zeebe.exporter;

public class ElasticsearchExporterConfiguration {

  private String host = "localhost";
  private int port = 9200;
  private String clusterName = "elasticsearch";

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  @Override
  public String toString() {
    return "ElasticsearchExporterConfiguration{"
        + "host='"
        + host
        + '\''
        + ", port="
        + port
        + ", clusterName='"
        + clusterName
        + '\''
        + '}';
  }
}

/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter;

public class ElasticsearchExporterConfiguration {

  private String host = "localhost";
  private int port = 9300;
  private String clusterName = "elasticsearch";
  private boolean createTemplate = true;
  private String templateName = "zeebe-records";

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

  public boolean isCreateTemplate() {
    return createTemplate;
  }

  public void setCreateTemplate(boolean createTemplate) {
    this.createTemplate = createTemplate;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
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
        + ", createTemplate="
        + createTemplate
        + ", templateName='"
        + templateName
        + '\''
        + '}';
  }
}

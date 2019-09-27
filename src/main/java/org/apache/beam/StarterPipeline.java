/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.model.SchemaParser;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import  org.apache.beam.model.BigQueryConverters;
import javax.xml.bind.annotation.*;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.*;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectPipelineRunner, just
 * execute it without any additional parameters from your favorite development
 * environment. In Eclipse, this corresponds to the existing 'LOCAL' run
 * configuration.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=BlockingDataflowPipelineRunner
 * In Eclipse, you can just modify the existing 'SERVICE' run configuration.
 */
@SuppressWarnings("serial")
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

//    String root = "roor";
//    String recordElement = "root";
//    String filename = "file.xml";

    String root = "offers";
    String recordElement = "shop";
    String filename = "ecrm.xml";

    Pipeline p = Pipeline.create(options);

    PCollection<Shop> xml = p.apply(XmlIO.<Shop>read()
            .withRootElement(root)
            .withRecordElement(recordElement)
            .withRecordClass(Shop.class)
            .from(Paths.get(".", "Data", filename).toString()));

            xml.apply(ParDo.of(new DoFn<Shop, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {

                Shop shop = c.element();
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("shopId", shop.getShopId());
                data.put("campaignId", shop.getCampaign().getCampaignId());

                for (Article temp : shop.getCampaign().getArticles().getArticles()) {
//                  System.out.println(temp);
                  Map<String, Object> row = new HashMap<String, Object>();
                  row.putAll(data);
                  row.put("articleId", temp.getId());
                  row.put("lineName", temp.getLineName());

                  Gson gson = new Gson();
                  String json = gson.toJson(row);
                  c.output(json);
                } }
            }))
                    .apply(BigQueryConverters.jsonToTableRow())
                    .apply(
                            "Insert into Bigquery",
                            BigQueryIO.writeTableRows()
                                    .withSchema(
                                            ValueProvider.NestedValueProvider.of(
                                                    options.getJSONPath(),
                                                    new SerializableFunction<String, TableSchema>() {

                                                      @Override
                                                      public TableSchema apply(String jsonPath) {

                                                        TableSchema tableSchema = new TableSchema();
                                                        List<TableFieldSchema> fields = new ArrayList<>();
                                                        SchemaParser schemaParser = new SchemaParser();
                                                        JSONObject jsonSchema;

                                                        try {

                                                          jsonSchema = schemaParser.parseSchema(jsonPath);

                                                          JSONArray bqSchemaJsonArray =
                                                                  jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                                                          for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                                            JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                                            TableFieldSchema field =
                                                                    new TableFieldSchema()
                                                                            .setName(inputField.getString(NAME))
                                                                            .setType(inputField.getString(TYPE));

                                                            if (inputField.has(MODE)) {
                                                              field.setMode(inputField.getString(MODE));
                                                            }

                                                            fields.add(field);
                                                          }
                                                          tableSchema.setFields(fields);

                                                        } catch (Exception e) {
                                                          throw new RuntimeException(e);
                                                        }
                                                        return tableSchema;
                                                      }
                                                    }))
                                    .to(options.getOutputTable())
                                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                                    .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    ;

    p.run();
  }
}


@XmlRootElement(name = "record")
@XmlAccessorType(XmlAccessType.FIELD)
class Record implements Serializable {
  /**
   * Model Fields
   */
  private String id;

  Record() { }

  Record(String id) {
    this.id = id;
  }


  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}

@XmlRootElement(name = "root")
@XmlAccessorType(XmlAccessType.FIELD)
class Root implements Serializable {
  /**
   * Model Fields
   */
  @XmlElement(name = "record")
  private List<Record> records = null;


  public List<Record> getRecords() {
    return records;
  }

  public void setRecords(List<Record> records) {
    this.records = records;
  }
}

@XmlRootElement(name = "article")
@XmlAccessorType(XmlAccessType.FIELD)
class Article implements Serializable {
  /**
   * Model Fields
   */
  @XmlElement(name = "articleId")
  private String id;
  private String nameWithoutHTML;
  private String nameWithHTML;
  private String lineName;
  private String familyName;
  private String parentProductId;
  private String primaryCategoryId;
  private String benefit;
  private String catalogPrice;
  private String price;
  private String promotion;
  private String is2f1;
  private String storeGroup;
  private String isVisible;
  private String isAvailable;
  private String isTestimonialsEnabled;

  Article() {}

  Article(String id, String nameWithoutHTML, String nameWithHTML, String lineName, String familyName,
          String parentProductId, String primaryCategoryId, String benefit, String catalogPrice, String price,
          String promotion, String is2f1, String storeGroup, String isVisible, String isAvailable,
          String isTestimonialsEnabled) {
    this.id = id;
    this.nameWithoutHTML = nameWithoutHTML;
    this.nameWithHTML = nameWithHTML;
    this.lineName = lineName;
    this.familyName = familyName;
    this.parentProductId = parentProductId;
    this.primaryCategoryId = primaryCategoryId;
    this.benefit = benefit;
    this.catalogPrice = catalogPrice;
    this.price = price;
    this.promotion = promotion;
    this.is2f1 = is2f1;
    this.storeGroup = storeGroup;
    this.isVisible = isVisible;
    this.isAvailable = isAvailable;
    this.isTestimonialsEnabled = isTestimonialsEnabled;
  }


  public String getPrice() {
    return price;
  }

  public void setPrice(String price) {
    this.price = price;
  }

  public String getPromotion() {
    return promotion;
  }

  public void setPromotion(String promotion) {
    this.promotion = promotion;
  }

  public String getIs2f1() {
    return is2f1;
  }

  public void setIs2f1(String is2f1) {
    this.is2f1 = is2f1;
  }

  public String getStoreGroup() {
    return storeGroup;
  }

  public void setStoreGroup(String storeGroup) {
    this.storeGroup = storeGroup;
  }

  public String getIsVisible() {
    return isVisible;
  }

  public void setIsVisible(String isVisible) {
    this.isVisible = isVisible;
  }

  public String getIsAvailable() {
    return isAvailable;
  }

  public void setIsAvailable(String isAvailable) {
    this.isAvailable = isAvailable;
  }

  public String getIsTestimonialsEnabled() {
    return isTestimonialsEnabled;
  }

  public void setIsTestimonialsEnabled(String isTestimonialsEnabled) {
    this.isTestimonialsEnabled = isTestimonialsEnabled;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getNameWithoutHTML() {
    return nameWithoutHTML;
  }

  public void setNameWithoutHTML(String nameWithoutHTML) {
    this.nameWithoutHTML = nameWithoutHTML;
  }

  public String getNameWithHTML() {
    return nameWithHTML;
  }

  public void setNameWithHTML(String nameWithHTML) {
    this.nameWithHTML = nameWithHTML;
  }

  public String getLineName() {
    return lineName;
  }

  public void setLineName(String lineName) {
    this.lineName = lineName;
  }

  public String getFamilyName() {
    return familyName;
  }

  public void setFamilyName(String familyName) {
    this.familyName = familyName;
  }

  public String getParentProductId() {
    return parentProductId;
  }

  public void setParentProductId(String parentProductId) {
    this.parentProductId = parentProductId;
  }

  public String getPrimaryCategoryId() {
    return primaryCategoryId;
  }

  public void setPrimaryCategoryId(String primaryCategoryId) {
    this.primaryCategoryId = primaryCategoryId;
  }

  public String getBenefit() {
    return benefit;
  }

  public void setBenefit(String benefit) {
    this.benefit = benefit;
  }

  public String getCatalogPrice() {
    return catalogPrice;
  }

  public void setCatalogPrice(String catalogPrice) {
    this.catalogPrice = catalogPrice;
  }
}

@XmlRootElement(name = "articles")
@XmlAccessorType(XmlAccessType.FIELD)
class Articles implements Serializable {
  /**
   * Model Fields
   */
  @XmlElement(name = "article")
  private List<Article> articles = null;

  public List<Article> getArticles() {
    return articles;
  }

  public void setArticles(List<Article> articles) {
    this.articles = articles;
  }
}

@XmlRootElement(name = "campaign")
@XmlAccessorType(XmlAccessType.FIELD)
class Campaign implements Serializable {
  /**
   * Model Fields
   */
  private String campaignId;
  private Articles articles;

  Campaign() { }

  public String getCampaignId() {
    return campaignId;
  }

  public void setCampaignId(String campaignId) {
    this.campaignId = campaignId;
  }

  public Articles getArticles() {
    return articles;
  }

  public void setArticles(Articles articles) {
    this.articles = articles;
  }
}


@XmlRootElement(name = "shop")
@XmlAccessorType(XmlAccessType.FIELD)
class Shop implements Serializable {
  /**
   * Model Fields
   */
  @XmlElement(name = "shopId")
  private String shopId;

  @XmlElement(name = "campaign")
  private Campaign campaign;

  Shop() { }


  public Campaign getCampaign() {
    return campaign;
  }

  public void setCampaign(Campaign campaign) {
    this.campaign = campaign;
  }

  public String getShopId() {
    return shopId;
  }

  public void setShopId(String shopId) {
    this.shopId = shopId;
  }
}
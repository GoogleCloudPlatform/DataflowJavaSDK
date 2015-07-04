# README #

### springML Inc Repository ###

* Salesforce platform custom Source
* Salesforce Wave custom Sink

Google Dataflow Jobs
--------------------
Following two classes showcase usage of custom Source and Sink

SFReferenceDataJob - fetches reference data from Salesforce platform and populates bigQuery tables
AdDataJob - Fetches raw data from GCS and enhances it with previously obtained Salesforce reference data

The following Salesforce jars are needed to execute the jobs: enterprise.jar, partner.jar and force-wsc-34.0.0-uber.jar.






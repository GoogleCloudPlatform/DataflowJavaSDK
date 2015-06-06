# README #

### springML Inc Repository ###

* Prototypes
* Sample code

Google Dataflow Jobs
--------------------


Following two classes take care of Google cloud dataflow jobs

SFReferenceDataJob - Will fetch the reference data from SF (Oppurtunity) and populate bigQuery
AdDataJob - Will fetch the raw data from GCS and SF reference data from bigquery. Enrich the data and populate bigQuery with the enriched data


SFReferenceDataJob
------------------

This requires the following inputs

1. Google cloud project - "ace-scarab-94723" used as default
2. Google cloud Staging location - "gs://sam-bucket1/staging" is default value
3. BigQuery output table - "ace-scarab-94723:SFDCReferenceData.SFRef" is default value
4. SF UserId - demoanalytics@gmail.com is default
5. SF Password - Fire2015!yJn8QwkmqcbFhqIiwieXkMTe is default

On completion of the job, bigquery table SFDCReferenceData.SFRef will be populated with SF Reference data


AdDataJob
---------

This requires the following inputs

1. Google cloud project - "ace-scarab-94723" used as default
2. Google cloud Staging location - "gs://sam-bucket1/staging" is default value
3. Ad Raw data (CSV) - gs://sam-bucket1/SampleAdData/ad-server-data1.csv is default value
4. BigQuery Reference data table - ace-scarab-94723:SFDCReferenceData.SFRef is defalt value
5. BigQuery output table - "ace-scarab-94723:SFDCReferenceData.EnrichedSample" is default value

On completion of the job bigquery table SFDCReferenceData.EnrichedSample will be populated withenriched data.


Since the local jars enterprise.jar, partner.jar and force-wsc-34.0.0-uber.jar needs to be in classpath, I've not used maven to execute the jobs. Instead I executed in eclipse itself. 
Either you use your IDE or execute it from command line. In command line make sure you have the required jars in classpath






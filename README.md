# Scenario

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Assignment

Build an ETL pipeline that extracts JSON log data from an S3 bucket, for this I'm using my local machine as the server to load that data into create my tables and run queries then create my staging tables After I stage the aggregated data into Redshift cluster for any BI to utilize it.

# Set up

In the `dwh.cfg` file you will need to create and grant access to your AWS IAM account for the use of this application. This will allow `boto3` python package to do the setup of the inital infrastructure in your AWS account.
# Aquarius Time Series (AQTS) Capture Trigger
[![Build Status](https://travis-ci.org/usgs/aqts-capture-trigger.svg?branch=master)](https://travis-ci.org/usgs/aqts-capture-trigger)
[![codecov](https://codecov.io/gh/usgs/aqts-capture-trigger/branch/master/graph/badge.svg)](https://codecov.io/gh/usgs/aqts-capture-trigger)

AWS Lambda function designed to trigger the AQTS Capture State Machine.

## Updating the S3 to RDS Role
Any time you deploy this service, you need to make sure the S3 to RDS Role is assigned to the RDS Cluster.
Right now, this is a manual step:

RDS->Databases->nwcapture-<tier>

Add IAM Roles to this cluster: aqts-capture-trigger-<tier>-NwCaptureS3ToRDS-Transfer<Random String>
Feature: s3Import

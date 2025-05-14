ir-exportq Queue
======================

This queue task can run daily and exports all of the records in IR Scholar to a CSV file and uploads the CSV file to a S3 bucket.

It requires 2 environmental variables to deteremine the destination S3 bucket and the Scholar url (without the ending '/') to query.
  ***S3_BUCKET_IR_REPORT*** defaults to *cubl-ir-reports* if not set.
  ***SCHOLAR_URL*** defaults to *https://scholar.colorado.edu* if not set.

License
-------
University Libraries - University Of Colorado - Boulder

Author Information
------------------
University Libraries - University Of Colorado - Boulder

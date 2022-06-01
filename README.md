# hls-landsat-historic

AWS Stack for querying historic Landsat inventory and notifying [hls-orchestration](https://github.com/nasa-impact/hls-orchestration)

## Requirements
- Python>=3.8
- tox
- aws-cli
- An IAM role with sufficient permissions for creating, destroying and modifying the relevant stack resources.

## Environment Settings
```
$ export LANDSAT_HISTORIC_STACKNAME=<Name of your stack>
$ export LANDSAT_HISTORIC_LAST_DATE_INITIAL=<The date to start backwards historic processing YYYY/MM/DD>
$ export LANDSAT_HISTORIC_DAYS_RANGE=<The number of days to process in each execution >
$ export LANDSAT_HISTORIC_PLATFORM=<8 or 9 depending on whether
processing Landsat-8 or Landsat-9>
$ export LANDSAT_HISTORIC_CRON_STRING=<AWS Cron string for execution frequency>
$ export LANDSAT_HISTORIC_GCC=<False>
$ export LANDSAT_HISTORIC_GCC_BOUNDARY_ARN=<GCC Boundary Policy Arn>

```

## CDK Commands
### Synth
Display generated cloud formation template that will be used to deploy.
```
$ tox -e dev -r -- synth
```

### Diff
Display a diff of the current deployment and any changes created.
```
$ tox -e dev -r -- diff || true
```

### Deploy
Deploy current version of stack.
```
$ tox -e dev -r -- deploy
```

## Inventory Data
The subset_granules Lambda functions requires access to the USGS Historic Landsat Invetory.
The USGS Historic Landsat Invetory is available via
```
$ s3://usgs-landsat/collection02/inventory/inventory_product_list.zip --request-payer requester
```
S3 Select only supports [GZIP and BZIP2](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html) compression.  Recompress the inventory with
```
$ unzip inventory_product_list.zip
$ gzip inventory_product_list.csv
```
It can then be uploaded to the bucket created by your stack deploy
```
$ s3 cp inventory_product_list.csv.gz s3://<bucket_name>
```

## Development
For active stack development run
```
$ tox -e dev -r -- version
```
This creates a local virtualenv in the directory `devenv`.  To use it for development
```
$ source devenv/bin/activate
```
Then run the following to install the project's pre-commit hooks
```
$ pre-commit install
```

## Tests
To run unit test for all included Lambda functions
```
tox -r
```

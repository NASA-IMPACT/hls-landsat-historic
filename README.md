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

### Inventory Data
The subset_granules Lambda functions requires access to the USGS Historic Landsat Invetory.
The USGS Historic Landsat Invetory is available via
```
$ s3://usgs-landsat/collection02/inventory/inventory_product_list.zip --request-payer requester
```
S3 Select only supports [GZIP and BZIP2](https://docs.aws.amazon.com/AmazonS3/latest/userguide/selecting-content-from-objects.html) compression.  Recompress the inventory with
```
$ unzip inventory_product_list.zip
$ gzip inventory_product_list.json
```
It can then be uploaded to the bucket created by your stack deploy
```
$ s3 cp inventory_product_list.json.gz s3://<bucket_name>
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

## Tests
To run unit test for all included Lambda functions
```
tox -r
```
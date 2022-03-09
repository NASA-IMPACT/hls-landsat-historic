import os

from aws_cdk import (
    aws_events,
    aws_events_targets,
    aws_iam,
    aws_lambda,
    aws_lambda_python,
    aws_s3,
    aws_sns,
    aws_ssm,
    core,
)

LAST_DATE_INITIAL = os.getenv("LANDSAT_HISTORIC_LAST_DATE_INITIAL")
DAYS_RANGE = os.getenv("LANDSAT_HISTORIC_DAYS_RANGE")
CRON_STRING = os.getenv("LANDSAT_HISTORIC_CRON_STRING")
USGS_S3_LANDSAT_INVENTORY_BUCKET = os.getenv("LANDSAT_HISTORIC_INVENTORY_BUCKET")
INV_DL_CRON_STRING = os.getenv("LANDSAT_HISTORIC_INVENTORY_DL_CRON_STRING")

if os.getenv("LANDSAT_HISTORIC_GCC", None) == "true":
    GCC = True
else:
    GCC = False


class LandsatHistoricStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_name: str, **kwargs) -> None:
        super().__init__(scope, stack_name, **kwargs)

        if GCC:
            boundary_arn = os.environ["LANDSAT_HISTORIC_GCC_BOUNDARY_ARN"]
            from permission_boundary import PermissionBoundaryAspect

            self.node.apply_aspect(PermissionBoundaryAspect(boundary_arn))

        self.landsat_inventory_bucket = aws_s3.Bucket(
            self,
            "LandsatInventoryBucket",
            bucket_name=f"{stack_name}-inventory-bucket",
        )

        self.topic = aws_sns.Topic(
            self, "LandsatHistoricTopic", display_name="Landsat Historic Topic"
        )

        self.role = aws_iam.Role(
            self,
            "LandsatHistoricFunction",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        last_date_parameter_name = f"{stack_name}_last_date_parameter"
        self.last_date_parameter = aws_ssm.StringParameter(
            self,
            last_date_parameter_name,
            string_value=LAST_DATE_INITIAL,
            parameter_name=last_date_parameter_name,
        )

        self.rule = aws_events.Rule(
            self,
            f"{stack_name}_cron_rule",
            schedule=aws_events.Schedule.expression(CRON_STRING),
        )

        self.subset_granules_function = aws_lambda_python.PythonFunction(
            self,
            id=f"{stack_name}-subset-granules-function",
            entry="lambdas",
            handler="handler",
            index="subset_granules.py",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=5000,
            timeout=core.Duration.minutes(15),
            role=self.role,
            environment={
                "BUCKET": self.landsat_inventory_bucket.bucket_name,
                "KEY": "inventory_product_list.json.gz",
                "TOPIC_ARN": self.topic.topic_arn,
                "LAST_DATE_PARAMETER_NAME": last_date_parameter_name,
                "DAYS_RANGE": DAYS_RANGE,
            },
        )

        self.rule.add_target(
            aws_events_targets.LambdaFunction(self.subset_granules_function)
        )

        self.historic_dl_rule = aws_events.Rule(
            self,
            f"{stack_name}_inventory_dl_cron_rule",
            schedule=aws_events.Schedule.expression(INV_DL_CRON_STRING),
        )

        self.download_historic_usgs_csv = aws_lambda_python.PythonFunction(
            self,
            id=f"{stack_name}-download_historic_usgs_csv",
            entry="lambdas",
            handler="handler",
            index="download_historic_landsat_csv.py",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=5000,
            timeout=core.Duration.minutes(15),
            role=self.role,
            environment={
                "BUCKET": self.landsat_inventory_bucket.bucket_name,
                "KEY": "inventory_product_list.zip",
                "COPY_BUCKET": USGS_S3_LANDSAT_INVENTORY_BUCKET,
            },
        )

        self.historic_dl_rule.add.add_target(
            aws_events_targets.LambdaFunction(self.download_historic_usgs_csv)
        )

        self.landsat_inventory_bucket.grant_read(self.role)
        self.topic.grant_publish(self.role)
        self.last_date_parameter.grant_read(self.role)
        self.last_date_parameter.grant_write(self.role)

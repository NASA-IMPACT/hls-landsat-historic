import os

from aws_cdk import (
    aws_events,
    aws_events_targets,
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


class LandsatHistoricStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_name: str, **kwargs) -> None:
        super().__init__(scope, stack_name, **kwargs)

        self.landsat_inventory_bucket = aws_s3.Bucket(
            self,
            "LandsatInventoryBucket",
            bucket_name=f"{stack_name}-inventory-bucket",
        )

        self.topic = aws_sns.Topic(
            self, "LandsatHistoricTopic", display_name="Landsat Historic Topic"
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

        self.landsat_inventory_bucket.grant_read(self.subset_granules_function)
        self.topic.grant_publish(self.subset_granules_function)

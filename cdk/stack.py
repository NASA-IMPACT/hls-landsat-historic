import os

from aws_cdk import aws_iam, aws_lambda, aws_lambda_python, aws_s3, aws_sns, core


class LandsatHistoricStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_name: str, **kwargs) -> None:
        super().__init__(scope, stack_name, **kwargs)

        print(dir(aws_s3))
        self.landsat_inventory_bucket = aws_s3.Bucket(
            self,
            "LandsatInventoryBucket",
            bucket_name=f"{stack_name}-inventory-bucket",
        )

        self.topic = aws_sns.Topic(
            self, "LandsatHistoricTopic", display_name="Landsat Historic Topic"
        )

        self.role = aws_iam.Role.from_role_arn(self, 
                "LandsatHistoric",
                "arn:aws:iam::611670965994:role/HLS-lambda-role",
                mutable=False
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
            },
        )

        self.topic.grant_publish(self.subset_granules_function)

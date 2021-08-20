import os

from aws_cdk import (
    aws_s3,
    aws_lambda,
    aws_lambda_python,
    core,
)


class LandsatHistoricStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_name: str, **kwargs) -> None:
        super().__init__(scope, stack_name, **kwargs)

        self.landsat_inventory_bucket = aws_s3.Bucket(
            self,
            "LandsatInventoryBucket",
            bucket_name=f"{stack_name}-inventory-bucket",
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
            }
        )

        self.landsat_inventory_bucket.grant_read(self.subset_granules_function)

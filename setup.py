from setuptools import setup, find_packages

# runtime requirements.
aws_cdk_version = "1.65.0"
aws_cdk_reqs = [
    "core",
    "aws-s3",
    "aws-iam",
    "aws-lambda",
    "aws-sns",
    "aws-lambda-python",
]

inst_reqs = [
    "boto3",
]

inst_reqs.append([f"aws_cdk.{x}=={aws_cdk_version}" for x in aws_cdk_reqs])

extra_reqs = {
    "test": ["pytest", "pytest-cov", "black", "flake8"],
    "dev": ["pytest", "black", "flake8", "nodeenv"]
}

setup(
    name="hls-landsat-historic",
    version="0.0.1",
    python_requires=">=3.7",
    author="development seed",
    packages=find_packages(),
    package_data={
        ".": [
            "cdk.json",
        ],
    },
    install_requires=inst_reqs,
    extras_require=extra_reqs,
    include_package_data=True,
)

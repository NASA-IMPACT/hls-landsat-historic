from setuptools import find_packages, setup

# runtime requirements.
aws_cdk_version = "1.65.0"
aws_cdk_reqs = [
    "core",
    "aws-s3",
    "aws-iam",
    "aws-lambda",
    "aws-sns",
    "aws-lambda-python",
    "aws-ssm",
    "aws-events",
    "aws-events-targets",
]

inst_reqs = [
    "boto3",
]

inst_reqs.append([f"aws_cdk.{x}=={aws_cdk_version}" for x in aws_cdk_reqs])

extra_reqs = {
    "test": ["pytest", "pytest-cov", "black", "flake8"],
    "dev": [
        "pytest",
        "black",
        "flake8",
        "nodeenv",
        "isort",
        "pre-commit",
        "pre-commit-hooks",
    ],
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

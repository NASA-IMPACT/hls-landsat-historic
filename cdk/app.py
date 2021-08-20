import os
from aws_cdk import core
from stack import LandsatHistoricStack


# Required env settings
STACKNAME = os.environ["LANDSAT_HISTORIC_STACKNAME"]

app = core.App()
LandsatHistoricStack(
    scope=app,
    stack_name=STACKNAME
)

for k, v in {
    "Project": "hls",
    "Stack": STACKNAME,
}.items():
    core.Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()

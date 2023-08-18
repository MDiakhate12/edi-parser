## Preprocessing Lambda
Code for the pre-processing and post-processing lambda function.

__Important__: All modules referenced by the lambda function need to be included in the `MANIFEST.in` file.

For testing locally, instructions to setup SAM are given below.

## CICD
The `pipeline.yml` file configures the CICD pipeline for building a zip file that will be deployed using XLDeploy. 

__Important__: The CICD pipeline creates the zip file by using the `python setup.py sdist --format=zip` bash command. This creates an extra layer on the zip package that is not expected by AWS (see [Working with .zip file archives for Python Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-searchpath)). For this reason the following lines needs to be added in the very beginning of `main.py` file:
```
import sys
sys.path.insert(0, "/var/task/Pre-processing-CICD-1")
```

## SAM
The SAM framework provided by AWS allows us to test our lambda function locally. For this you need to install:
- [Docker](https://docs.docker.com/get-docker/)
- [AWS SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

Then make sure Docker is working. In order to compile the lambda function locally go to the root folder of this repository and run the following command:
```
sam build --hook-name terraform --beta-features
```
This command has to be applied everytime you modify your lambda function code.

To test the function locally run the following command:
```
sam local invoke aws_lambda_function.optistow_preprocessing_dev -e events/event.json --beta-features --profile sandbox
```

__Important__: If your lambda function calls resources from AWS, even when deployed locally it will try to connect to AWS. For this reason you might need to setup your AWS credentials even if working locally ([see the docs](https://docs.aws.amazon.com/cli/latest/userguide/sso-configure-profile-token.html#sso-configure-profile-token-auto-sso)).
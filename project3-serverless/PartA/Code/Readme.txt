To allow our AWS Lambda function to access S3 and to view logs, we first need to create a role in IAM console by attaching LambaS3FullAccess and CloudWatchFullAccess

Next, create a function and add this role to the function via execution role. Code the function in code inline of AWS Lambda.

To trigger the function whenever a new object is added into S3 bucket, add a S3 trigger from designer tab and configure it by giving the name of the bucket and event type.

Our Lambda function name is lambda_function.py. This function preprocesses the content of every text file whenever the file gets uploaded into the bucket and outputs the transformed content to another S3 bucket.

Setup AWS CLI in your system and then sync the s3 bucket to a directory and apply the word_count.py to get the frequency of words in descending order


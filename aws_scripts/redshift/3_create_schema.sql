create external schema schema_justwatch
from data catalog 
database 'justwatch' 
iam_role 'arn:aws:iam::<YOUR_ACCOUNT_ID>:role/<YOUR_REDSHIFT_ROLE>' 
create external database if not exists;
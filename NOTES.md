
AWS CLI: Using a different storage provider than S3
---------------------------------------------------
The AWS CLI can be used with other S3-compatible storage providers like
Backblaze's B2 or Cloudflare's R2 by using the `--endpoint-url` argument
and setting `~/.aws/credentials` to the credentials for that provider:

    aws s3 --endpoint-url https://s3.eu-central-003.backblazeb2.com ls malthe/bitum_test/

Alternately, you can set `endpoint_url = <ENDPOINT_URL>` directly in `~/.aws/credentials`
in which case you don't have to pass the argument on the command line.

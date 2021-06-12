# delete_s3_object_golang

1. Run AWS CLI to save objects to `results.json` file
```
aws s3api list-objects --bucket <BUCKET_NAME> > results.json
```
2. The script will find older-than-28-day files which have extension `.png`, not `.pdf` and `.csv`, with an assumption that all files not having file extension are PNG files
```
go mod init
go mod tidy
go run main.go
```
3. Run go script with flag `--dryrun=false` to find the actual PNG files in the bucket and delete all PNG files
```
go run main.go --dryrun=false
```

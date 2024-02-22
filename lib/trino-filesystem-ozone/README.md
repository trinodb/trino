export AWS_ACCESS_KEY_ID=testuser/scm@EXAMPLE.COM
export AWS_SECRET_ACCESS_KEY=xyz

https://github.com/apache/ozone/blob/ozone-1.4.0/hadoop-hdds/docs/content/start/Minikube.md

aws s3api --endpoint http://192.168.49.2:30194 create-bucket --bucket=wordcount


aws s3api --endpoint http://192.168.49.2:30194 create-bucket --bucket=bucket1
ls -1 > /tmp/testfile

aws s3api --endpoint http://192.168.49.2:30194 put-object --bucket bucket1 --key s3://bucket1/testfile --body /tmp/testfile


aws s3api --endpoint http://192.168.49.2:30194 list-objects --bucket bucket1
```
{
    "Contents": [
        {
            "Key": "s3://bucket1/testfile",
            "LastModified": "2024-02-22T11:20:13.038000+00:00",
            "ETag": "2024-02-22T11:20:13.038Z",
            "Size": 246,
            "StorageClass": "STANDARD"
        }
    ],
    "RequestCharged": null
}
(
```



➜  minikube minikube service s3g-public
|-----------|------------|-------------|---------------------------|
| NAMESPACE |    NAME    | TARGET PORT |            URL            |
|-----------|------------|-------------|---------------------------|
| default   | s3g-public | rest/9878   | http://192.168.49.2:30194 |
|-----------|------------|-------------|---------------------------|
🎉  Opening service default/s3g-public in default browser...
➜  minikube Gtk-Message: 22:09:50.897: Not loading module "atk-bridge": The functionality is provided by GTK natively. Please try to not load it.

➜  minikube minikube service om-public
|-----------|-----------|----------------|---------------------------|
| NAMESPACE |   NAME    |  TARGET PORT   |            URL            |
|-----------|-----------|----------------|---------------------------|
| default   | om-public | ui/9874        | http://192.168.49.2:31849 |
|           |           | hadooprpc/9862 | http://192.168.49.2:31193 |
|-----------|-----------|----------------|---------------------------|
[default om-public ui/9874
hadooprpc/9862 http://192.168.49.2:31849
http://192.168.49.2:31193]

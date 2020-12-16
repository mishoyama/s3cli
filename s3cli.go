package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Cli represent a S3Cli Client
type S3Cli struct {
	profile    string // profile in credentials file
	endpoint   string // Server endpoine(URL)
	ak         string // access-key
	sk         string // secret-key
	region     string
	presign    bool // just presign
	presignExp time.Duration
	verbose    bool
	debug      bool
	Client     *s3.Client // manual init this field
}

// presignV2 presigne URL with escaped key(Object name).
func (sc *S3Cli) presignV2(method, bucketKey, contentType string) (string, error) {
	if bucketKey == "" || bucketKey[0] == '/' {
		return "", fmt.Errorf("invalid bucket/key: %s", bucketKey)
	}
	secret := aws.Credentials{}
	u, err := url.Parse(sc.endpoint)
	if err != nil {
		return "", err
	}
	exp := strconv.FormatInt(time.Now().Unix()+int64(sc.presignExp.Seconds()), 10)

	q := u.Query()
	q.Set("AWSAccessKeyId", secret.AccessKeyID)
	q.Set("Expires", exp)
	u.Path = fmt.Sprintf("/%s", bucketKey)

	contentMd5 := "" // header Content-MD5
	strToSign := fmt.Sprintf("%s\n%s\n%s\n%v\n%s", method, contentMd5, contentType, exp, u.EscapedPath())

	mac := hmac.New(sha1.New, []byte(secret.SecretAccessKey))
	mac.Write([]byte(strToSign))

	q.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// presignV2Raw presigne URL with raw key(Object name).
func (sc *S3Cli) presignV2Raw(method, bucketKey, contentType string) (string, error) {
	if bucketKey == "" || bucketKey[0] == '/' {
		return "", fmt.Errorf("invalid bucket/key: %s", bucketKey)
	}

	secret := aws.Credentials{}

	u, err := url.Parse(fmt.Sprintf("%s/%s", sc.endpoint, bucketKey))
	if err != nil {
		return "", err
	}
	exp := strconv.FormatInt(time.Now().Unix()+int64(sc.presignExp.Seconds()), 10)

	q := u.Query()
	q.Set("AWSAccessKeyId", secret.AccessKeyID)
	q.Set("Expires", exp)

	contentMd5 := "" // header Content-MD5
	strToSign := fmt.Sprintf("%s\n%s\n%s\n%v\n%s", method, contentMd5, contentType, exp, u.EscapedPath())

	mac := hmac.New(sha1.New, []byte(secret.SecretAccessKey))
	mac.Write([]byte(strToSign))

	q.Set("Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// bucketCreate create a Bucket
func (sc *S3Cli) bucketCreate(buckets []string) error {
	for _, b := range buckets {
		params := &s3.CreateBucketInput{
			Bucket: aws.String(b),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(sc.region),
			},
		}
		resp, err := sc.Client.CreateBucket(context.Background(), params)
		if err != nil {
			return err
		}
		if sc.verbose {
			fmt.Println(resp)
		}
	}
	return nil
}

// bucketList list all my Buckets
func (sc *S3Cli) bucketList() error {
	resp, err := sc.Client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil {
		return err
	}
	if sc.verbose {
		fmt.Println(resp)
		return nil
	}
	for _, b := range resp.Buckets {
		fmt.Println(*b.Name)
	}
	return nil
}

// bucketHead head a Bucket
func (sc *S3Cli) bucketHead(bucket string) error {
	resp, err := sc.Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return err
	}
	if resp != nil {
		fmt.Println(resp)
	}
	return err
}

// bucketACLGet get a Bucket's ACL
func (sc *S3Cli) bucketACLGet(bucket string) error {
	resp, err := sc.Client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return err
	}
	if resp != nil {
		fmt.Println(resp)
	}
	return err
}

// bucketACLSet set a Bucket's ACL
func (sc *S3Cli) bucketACLSet(bucket string, acl types.BucketCannedACL) error {
	resp, err := sc.Client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
		ACL:    acl,
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return err
	}
	if resp != nil {
		fmt.Println(resp)
	}
	return err
}

// bucketPolicyGet get a Bucket's Policy
func (sc *S3Cli) bucketPolicyGet(bucket string) error {
	resp, err := sc.Client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return err
	}
	fmt.Println(*resp.Policy)
	return nil
}

// bucketPolicySet set a Bucket's Policy
func (sc *S3Cli) bucketPolicySet(bucket, policy string) error {
	if policy == "" {
		return errors.New("empty policy")
	}

	resp, err := sc.Client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policy),
	})

	if err != nil {
		return err
	}
	fmt.Println(*resp)
	return nil
}

// bucketVersioningGet get a Bucket's Versioning status
func (sc *S3Cli) bucketVersioningGet(bucket string) error {
	resp, err := sc.Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		return err
	}
	fmt.Printf("BucketVersioning: %s\n", resp)
	return nil
}

// bucketVersioningSet set a Bucket's Versioning status
func (sc *S3Cli) bucketVersioningSet(bucket string, status types.BucketVersioningStatus) error {
	resp, err := sc.Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: status,
		},
	})

	if err != nil {
		return err
	}
	fmt.Printf("BucketVersioning: %s\n", resp)
	return nil
}

// bucketDelete delete a Bucket
func (sc *S3Cli) bucketDelete(bucket string) error {
	_, err := sc.Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	return err
}

// putObject upload a Object
func (sc *S3Cli) putObject(bucket, key string, r io.ReadSeeker) error {
	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if !reflect.ValueOf(r).IsNil() {
		putObjectInput.Body = r
	}

	if sc.presign {
		pc := s3.NewPresignClient(sc.Client)
		s, err := pc.PresignPutObject(context.Background(), putObjectInput)
		if err == nil {
			fmt.Println(s)
		}
		return err
	}

	resp, err := sc.Client.PutObject(context.Background(), putObjectInput)
	//resp, err := req.Send(context.Background())
	if err != nil {
		return err
	}
	if sc.verbose {
		fmt.Println(resp)
	}
	return nil
}

// headObject head a Object
func (sc *S3Cli) headObject(bucket, key string, mtime, mtimestamp bool) error {
	output, err := sc.Client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return err
	}

	if output == nil {
		return nil
	}
	if sc.verbose {
		fmt.Println(output)
	} else if mtime {
		fmt.Println(output.LastModified)
	} else if mtimestamp {
		fmt.Println(output.LastModified.Unix())
	} else {
		fmt.Printf("%d\t%s\n", output.ContentLength, output.LastModified)
	}
	return nil
}

// getObjectACL get A Object's ACL
func (sc *S3Cli) getObjectACL(bucket, key string) error {
	output, err := sc.Client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return err
	}
	if output != nil {
		fmt.Println(output)
	}
	return nil
}

// setObjectACL set A Object's ACL
func (sc *S3Cli) setObjectACL(bucket, key string, acl types.ObjectCannedACL) error {
	resp, err := sc.Client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		ACL:    acl,
	})

	if err != nil {
		return err
	}
	if resp != nil {
		fmt.Println(resp)
	}
	return nil
}

// listAllObjects list all Objects in specified bucket
func (sc *S3Cli) listAllObjects(bucket, prefix, delimiter string, index bool, startTime, endTime time.Time) error {

	return nil
}

// listAllObjectsV2 list all Objects in specified bucket
func (sc *S3Cli) listAllObjectsV2(bucket, prefix, delimiter string, index, owner bool, startTime, endTime time.Time) error {
	var i int64
	params := &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		Delimiter:  aws.String(delimiter),
		FetchOwner: owner,
	}

	p := s3.NewListObjectsV2Paginator(sc.Client, params)

	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("list all objects failed: %w", err)
		}
		if sc.verbose {
			fmt.Println(page)
			continue
		}
		for _, obj := range page.Contents {
			if obj.LastModified.Before(startTime) {
				continue
			}
			if obj.LastModified.After(endTime) {
				continue
			}

			if sc.verbose {
				fmt.Println(obj)
			} else if index {
				fmt.Printf("%d\t%s\n", i, *obj.Key)
				i++
			} else {
				fmt.Println(*obj.Key)
			}
		}
	}

	return nil
}

// listObjects (S3 listBucket)list Objects in specified bucket
func (sc *S3Cli) listObjects(bucket, prefix, delimiter, marker string, maxkeys int32, index bool, startTime, endTime time.Time) error {
	resp, err := sc.Client.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Marker:    aws.String(marker),
		Delimiter: aws.String(delimiter),
		MaxKeys:   maxkeys,
	})

	if err != nil {
		return fmt.Errorf("list objects failed: %w", err)
	}
	for _, p := range resp.CommonPrefixes {
		fmt.Println(*p.Prefix)
	}
	for i, obj := range resp.Contents {
		if obj.LastModified.Before(startTime) {
			continue
		}
		if obj.LastModified.After(endTime) {
			continue
		}
		if sc.verbose {
			fmt.Println(obj)
		} else if index {
			fmt.Printf("%d\t%s\n", i, *obj.Key)
		} else {
			fmt.Println(*obj.Key)
		}
	}
	return nil
}

// listObjectsV2 (S3 listBucket)list Objects in specified bucket
func (sc *S3Cli) listObjectsV2(bucket, prefix, delimiter, marker string, maxkeys int32, index, owner bool, startTime, endTime time.Time) error {
	resp, err := sc.Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucket),
		Prefix:     aws.String(prefix),
		StartAfter: aws.String(marker),
		Delimiter:  aws.String(delimiter),
		MaxKeys:    maxkeys,
		FetchOwner: owner,
	})

	if err != nil {
		return fmt.Errorf("list objects failed: %w", err)
	}
	for _, p := range resp.CommonPrefixes {
		fmt.Println(*p.Prefix)
	}
	for i, obj := range resp.Contents {
		if obj.LastModified.Before(startTime) {
			continue
		}
		if obj.LastModified.After(endTime) {
			continue
		}
		if sc.verbose {
			fmt.Println(obj)
		} else if index {
			fmt.Printf("%d\t%s\n", i, *obj.Key)
		} else {
			fmt.Println(*obj.Key)
		}
	}
	return nil
}

// listObjectVersions list Objects versions in Bucket
func (sc *S3Cli) listObjectVersions(bucket, prefix string) error {
	params := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	}
	if prefix != "" {
		params.Prefix = aws.String(prefix)
	}
	resp, err := sc.Client.ListObjectVersions(context.Background(), params)

	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}

	fmt.Println(resp)
	return nil
}

// getObject download a Object from bucket
func (sc *S3Cli) getObject(bucket, key, oRange, version string) (io.ReadCloser, error) {
	var objRange *string
	if oRange != "" {
		objRange = aws.String(fmt.Sprintf("bytes=%s", oRange))
	}
	var versionID *string
	if version != "" {
		versionID = aws.String(version)
	}
	params := &s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: versionID,
		Range:     objRange,
	}

	if sc.presign {
		pc := s3.NewPresignClient(sc.Client)
		s, err := pc.PresignGetObject(context.TODO(), params)
		if err == nil {
			fmt.Println(s.URL)
		}
		return nil, err
	}

	resp, err := sc.Client.GetObject(context.Background(), params)
	if err != nil {
		return nil, fmt.Errorf("get object failed: %w", err)
	}
	return resp.Body, nil

}

// catObject print Object contents
func (sc *S3Cli) catObject(bucket, key, oRange, version string) error {
	var objRange *string
	if oRange != "" {
		objRange = aws.String(fmt.Sprintf("bytes=%s", oRange))
	}
	var versionID *string
	if version != "" {
		versionID = aws.String(version)
	}
	params := &s3.GetObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: versionID,
		Range:     objRange,
	}

	if sc.presign {
		pc := s3.NewPresignClient(sc.Client)
		s, err := pc.PresignGetObject(context.Background(), params)
		if err == nil {
			fmt.Println(s.URL)
		}
		return err
	}

	resp, err := sc.Client.GetObject(context.Background(), params)
	if err != nil {
		return fmt.Errorf("get object failed: %w", err)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}

// renameObject rename Object
func (sc *S3Cli) renameObject(source, bucket, key string) error {
	// TODO: Copy and Delete Object
	return fmt.Errorf("not impl")
}

// copyObjects copy Object to destBucket/key
func (sc *S3Cli) copyObject(source, bucket, key string) error {
	params := &s3.CopyObjectInput{
		CopySource: aws.String(source),
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
	}

	resp, err := sc.Client.CopyObject(context.Background(), params)
	if err != nil {
		return fmt.Errorf("copy object failed: %w", err)
	}
	if sc.verbose {
		fmt.Println(resp)
		return nil
	}
	return nil
}

// deleteObjects list and delete Objects
func (sc *S3Cli) deleteObjects(bucket, prefix string) error {
	var objNum int64
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	for {
		resp, err := sc.Client.ListObjects(context.Background(), params)
		if err != nil {
			return fmt.Errorf("list object failed: %w", err)
		}
		objectNum := len(resp.Contents)
		if objectNum == 0 {
			break
		}
		if sc.verbose {
			fmt.Printf("Got %d Objects, ", objectNum)
		}
		objects := make([]types.ObjectIdentifier, 0, 1000)
		for _, obj := range resp.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
		}
		dparams := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Quiet:   true,
				Objects: objects,
			},
		}
		if _, e := sc.Client.DeleteObjects(context.Background(), dparams); err != nil {
			fmt.Printf("delete Objects failed: %s", e)
		} else {
			objNum = objNum + int64(objectNum)
		}
		if sc.verbose {
			fmt.Printf("%d Objects deleted\n", objNum)
		}

		if resp.NextMarker != nil {
			params.Marker = resp.NextMarker
		} else if resp.IsTruncated {
			params.Marker = resp.Contents[objectNum-1].Key
		} else {
			break
		}
	}
	return nil
}

// deleteBucketAndObjects force delete a Bucket
func (sc *S3Cli) deleteBucketAndObjects(bucket string, force bool) error {
	if force {
		if err := sc.deleteObjects(bucket, ""); err != nil {
			return err
		}
	}
	return sc.bucketDelete(bucket)
}

// deleteObject delete a Object(version)
func (sc *S3Cli) deleteObject(bucket, key, version string) error {
	var versionID *string
	if version != "" {
		versionID = aws.String(version)
	}
	resp, err := sc.Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket),
		Key:       aws.String(key),
		VersionId: versionID,
	})
	if err != nil {
		return err
	}
	if sc.verbose {
		fmt.Println(resp)
	}
	return nil
}

// mpuCreate create Multi-Part-Upload
func (sc *S3Cli) mpuCreate(bucket, key string) error {
	output, err := sc.Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return err
	}

	fmt.Println(output)
	return err
}

// mpuUpload do a Multi-Part-Upload
func (sc *S3Cli) mpuUpload(bucket, key, uid string, pid int32, filename string) error {
	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()
	resp, err := sc.Client.UploadPart(context.Background(), &s3.UploadPartInput{
		Body:       fd,
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		PartNumber: pid,
		UploadId:   aws.String(uid),
	})

	if err != nil {
		return err
	}

	fmt.Println(resp)
	return err
}

// mpuAbort abort Multi-Part-Upload
func (sc *S3Cli) mpuAbort(bucket, key, uid string) error {
	resp, err := sc.Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uid),
	})

	if err != nil {
		return err
	}

	fmt.Println(resp)
	return err
}

// mpuList list Multi-Part-Uploads
func (sc *S3Cli) mpuList(bucket, prefix string) error {
	var keyPrefix *string
	if prefix != "" {
		keyPrefix = aws.String(prefix)
	}
	resp, err := sc.Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: keyPrefix,
	})
	if err != nil {
		return err
	}

	fmt.Println(resp)
	return err
}

// mpuComplete completa Multi-Part-Upload
func (sc *S3Cli) mpuComplete(bucket, key, uid string, etags []string) error {
	parts := make([]types.CompletedPart, len(etags))
	for i, v := range etags {
		parts[i] = types.CompletedPart{
			PartNumber: int32(i + 1),
			ETag:       aws.String(v),
		}
	}
	resp, err := sc.Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
		UploadId: aws.String(uid),
	})

	if err != nil {
		return err
	}
	fmt.Println(resp)
	return err
}

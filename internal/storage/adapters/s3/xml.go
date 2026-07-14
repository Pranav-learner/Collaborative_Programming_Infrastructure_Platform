package s3

import "encoding/xml"

// s3Error is the standard S3 XML error document.
type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
	Key     string   `xml:"Key"`
	Bucket  string   `xml:"BucketName"`
}

// listBucketResult maps a ListObjectsV2 response.
type listBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken"`
	StartAfter            string         `xml:"StartAfter"`
	Contents              []listContents `xml:"Contents"`
}

type listContents struct {
	Key          string `xml:"Key"`
	Size         int64  `xml:"Size"`
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

// listAllBucketsResult maps a ListBuckets (service GET) response.
type listAllBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets struct {
		Bucket []struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		} `xml:"Bucket"`
	} `xml:"Buckets"`
}

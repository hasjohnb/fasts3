package s3wrapper

import (
	"bufio"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/AdRoll/goamz/s3"
)

const defaultDelimiter string = "/"

const numListRoutines int = 30

const maxRetries int = 5

type listWork struct {
	prefix    string
	delimiter string
}

// StripS3Path gets the key part from an s3 uri. The key part is everything after
// the header (s3://<bucket>)
func StripS3Path(key string) string {
	key = strings.TrimRight(key, "\n")
	if strings.Index(key, "s3://") < 0 {
		return key // IF s3:// is not located, assume the file is already stripped
	}

	// Take the first element after splitting from tab (Should be 's3://.../.../.../...')
	metaFilePath := strings.Split(key, "\t")[0]
	// Clear everything off the line before 's3://' to allow taking s3cmd ls output as input
	s3Path := metaFilePath[strings.Index(metaFilePath, "s3://"):]
	// The bucket is the second element if you split the string 's3://<bucket>/' by '/'
	bucket := strings.Split(s3Path, "/")[2]
	header := "s3://" + bucket
	return metaFilePath[strings.Index(metaFilePath, header)+len(header):]
}

// listWorkRecursion finds all the prefixes under prefix given searchDepth, throttleChan is a channel of booleans which limits the number
// of go routines to len(throttleChan), outChan is where the output list work is written, wg is a WaitGroup which tells the client
// when the function is complete, isRecursive tells the lister how to list the prefixes and keys
func listWorkRecursion(bucket *s3.Bucket, prefix string, searchDepth int, throttleChan chan bool, outChan chan s3.Key, wg *sync.WaitGroup, isRecursive bool) {
	wg.Add(1)
	throttleChan <- true
	go func() {
		// if the current depth has reached 1 we are finished
		if searchDepth == 0 {
			delimiter := ""
			if !isRecursive {
				delimiter = "/"
			}
			for listResp := range List(bucket, prefix, delimiter) {
				for _, prefix := range listResp.CommonPrefixes {
					outChan <- s3.Key{prefix, "", -1, "", "", s3.Owner{}}
				}
				for _, key := range listResp.Contents {
					outChan <- key
				}
			}
		} else {
			for res := range List(bucket, prefix, defaultDelimiter) {
				for _, key := range res.Contents {
					outChan <- key
				}

				if len(res.CommonPrefixes) != 0 {
					for _, newPfx := range res.CommonPrefixes {
						// release the current routine we are using as we will be blocked in the recursive call
						<-throttleChan
						listWorkRecursion(bucket, newPfx, searchDepth-1, throttleChan, outChan, wg, isRecursive)
						throttleChan <- true
					}
				} else { // if there are no common prefixes this is the end of the depth for this prefix
					listWorkRecursion(bucket, prefix, 0, throttleChan, outChan, wg, isRecursive)
				}
			}
		}
		wg.Done()
		<-throttleChan
	}()
}

// FastList does a recursive threaded operation to generate a list of prefixes based on prefix
// by searching down the searchDepth using DELIMITER as a delimiter and isRecursive to tell
// whether or not to use a delimiter when listing
func FastList(bucket *s3.Bucket, prefix string, searchDepth int, isRecursive bool) chan s3.Key {
	results := make(chan s3.Key, 100000)
	throttleChan := make(chan bool, numListRoutines)
	var wg sync.WaitGroup
	listWorkRecursion(bucket, prefix, searchDepth, throttleChan, results, &wg, isRecursive)
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}

// List function with retry and support for listing all keys in a prefix
func List(bucket *s3.Bucket, prefix string, delimiter string) <-chan *s3.ListResp {
	ch := make(chan *s3.ListResp, 10000)
	go func(pfix string, del string) {
		defer close(ch)
		isTruncated := true
		nextMarker := ""
		for isTruncated {
			attempts := 0
			for {
				attempts++
				res, err := bucket.List(pfix, del, nextMarker, 1000)
				if err != nil {
					if err.Error() == "runtime error: index out of range" {
						break
					}
					if attempts >= maxRetries {
						log.Panic(err)
					}

					time.Sleep(time.Second * 3)
				} else {
					ch <- res
					if len(res.Contents) > 0 {
						nextMarker = res.Contents[len(res.Contents)-1].Key
					} else if len(res.CommonPrefixes) > 0 {
						nextMarker = res.CommonPrefixes[len(res.CommonPrefixes)-1]
					}
					isTruncated = res.IsTruncated
					break
				}
			}
		}
	}(prefix, delimiter)
	return ch
}

func Put(bucket *s3.Bucket, key string, contents []byte, contentType string, permissions s3.ACL, options s3.Options) error {
	attempts := 0
	for {
		attempts++
		err := bucket.Put(key, contents, contentType, permissions, options)
		if err == nil {
			return nil
		}
		if attempts >= maxRetries && err != nil {
			return err
		}

		time.Sleep(time.Second * 3)
	}
}

func Get(bucket *s3.Bucket, key string) ([]byte, error) {
	attempts := 0
	for {
		attempts++
		buff, err := bucket.Get(key)
		if err == nil {
			return buff, nil
		}
		if attempts >= maxRetries && err != nil {
			return nil, err
		}
	}

}

func GetReader(bucket *s3.Bucket, key string) (io.ReadCloser, error) {
	attempts := 0
	for {
		attempts++
		reader, err := bucket.GetReader(key)
		if err == nil {
			return reader, nil
		}
		if attempts >= maxRetries && err != nil {
			return nil, err
		}
	}

}

func GetStream(bucket *s3.Bucket, key string) (*bufio.Reader, error) {
	bts, err := GetReader(bucket, key)
	if err != nil {
		return nil, err
	}
	return getReaderByExt(bts, key)
}

// getReaderByExt is a factory for reader based on the extension of the key
func getReaderByExt(reader io.ReadCloser, key string) (*bufio.Reader, error) {
	ext := path.Ext(key)
	if ext == ".gz" || ext == ".gzip" {
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return bufio.NewReader(reader), nil
		}
		return bufio.NewReader(gzReader), nil
	} else {
		return bufio.NewReader(reader), nil
	}
}

// createPathIfNotExists takes a path and creates
// it if it doesn't exist
func createPathIfNotExists(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return err
		}
	} else {
		return nil
	}
	return nil
}

// GetToFile takes a bucket and key and puts the bytes to a file
// in specified by dest
func GetToFile(bucket *s3.Bucket, key string, dest string) error {
	destParts := strings.Split(dest, "/")
	path := strings.Join(destParts[0:len(destParts)-1], "/")

	if err := createPathIfNotExists(path); err != nil {
		return err
	}

	bts, err := Get(bucket, key)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(dest, bts, 0644)
	if err != nil {
		return err
	}
	return nil
}

func toDeleteStruct(keys []string) s3.Delete {
	objs := make([]s3.Object, 0)
	for _, key := range keys {
		if key != "" {
			objs = append(objs, s3.Object{Key: strings.TrimSpace(key)})
		}
	}
	return s3.Delete{false, objs}
}

// DeleteMulti  deletes multiple keys
func DeleteMulti(bucket *s3.Bucket, keys []string) error {
	attempts := 0
	for {
		attempts++
		err := bucket.DelMulti(toDeleteStruct(keys))
		if err != nil {
			if attempts >= maxRetries {
				return err
			}

			time.Sleep(time.Second * 3)
		} else {
			break
		}
	}
	return nil
}

func Copy(bucket *s3.Bucket, srcKey string, destKey string) error {
	attempts := 0
	for {
		attempts++
		_, err := bucket.PutCopy(destKey, s3.PublicReadWrite, s3.CopyOptions{}, srcKey)
		if err != nil {
			if attempts >= maxRetries {
				return err
			}

			time.Sleep(time.Second * 3)
		} else {
			break
		}
	}
	return nil

}

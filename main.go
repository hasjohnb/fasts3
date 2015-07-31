package main

/**
 * A utility for doing operations on s3 faster than s3cmd
 */
import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/TuneOSS/fasts3/awswrapper"
	"github.com/TuneOSS/fasts3/s3wrapper"
	"github.com/alecthomas/kingpin"
	"github.com/dustin/go-humanize"
)

type s3List []string

// Set overrides kingping's Set method to validate value for s3 URIs
func (s *s3List) Set(value string) error {
	hasMatch, err := regexp.MatchString("^s3://", value)
	if err != nil {
		return err
	}
	if !hasMatch {
		return fmt.Errorf("%s not a valid S3 uri, Please enter a valid S3 uri. Ex: s3://mary/had/a/little/lamb\n", *lsS3Uri)
	} else {
		*s = append(*s, value)
		return nil
	}
}

func (s *s3List) String() string {
	return ""
}

// IsCumulative specifies S3List as a cumulative argument
func (s *s3List) IsCumulative() bool {
	return true
}

// S3List creates a new S3List kingpin setting
func S3List(s kingpin.Settings) (target *[]string) {
	target = new([]string)
	s.SetValue((*s3List)(target))
	return
}

var (
	app = kingpin.New("fasts3", "Multi-threaded s3 utility")

	ls            = app.Command("ls", "List s3 prefixes.")
	lsS3Uri       = ls.Arg("s3uri", "paritial s3 uri to list, ex: s3://mary/had/a/little/lamb/").Required().String()
	lsRecurse     = ls.Flag("recursive", "Get all keys for this prefix.").Short('r').Bool()
	lsSearchDepth = ls.Flag("search-depth", "search depth to search for work.").Default("0").Int()
	humanReadable = ls.Flag("human-readable", "human readable key size.").Short('H').Bool()
	withDate      = ls.Flag("with-date", "include the last modified date.").Short('d').Bool()

	del            = app.Command("del", "Delete s3 keys")
	delPrefixes    = S3List(del.Arg("prefixes", "1 or more partial s3 uris to delete delimited by space"))
	delRecurse     = del.Flag("recursive", "Delete all keys with prefix").Short('r').Bool()
	delSearchDepth = del.Flag("search-depth", "search depth to search for work.").Default("0").Int()

	get            = app.Command("get", "Fetch files from s3")
	getS3Uris      = S3List(get.Arg("prefixes", "list of prefixes or s3Uris to retrieve"))
	getSearchDepth = get.Flag("search-depth", "search depth to search for work.").Default("0").Int()
	getKeyRegex    = get.Flag("key-regex", "regex filter for keys").Default("").String()

	stream               = app.Command("stream", "Stream s3 files to stdout")
	streamS3Uris         = S3List(stream.Arg("prefixes", "list of prefixes or s3Uris to retrieve"))
	streamSearchDepth    = stream.Flag("search-depth", "search depth to search for work.").Default("0").Int()
	streamKeyRegex       = stream.Flag("key-regex", "regex filter for keys").Default("").String()
	streamIncludeKeyName = stream.Flag("include-key-name", "regex filter for keys").Bool()

	cp            = app.Command("cp", "Copy s3 files from one bucket to another")
	cpSrc         = cp.Arg("src", "prefix or s3Uri to copy").Required().String()
	cpDest        = cp.Arg("dest", "destination prefix or s3Uri to copy to").Required().String()
	cpSearchDepth = cp.Flag("search-depth", "search depth to search for work.").Default("0").Int()
	cpKeyRegex    = cp.Flag("key-regex", "regex filter for keys").Default("").String()

	initApp = app.Command("init", "Initialize .fs3cfg file in home directory")
)

// parseS3Uri parses a s3 uri into it's bucket and prefix
func parseS3Uri(s3Uri string) (bucket string, prefix string) {
	s3UriParts := strings.Split(s3Uri, "/")
	prefix = strings.Join(s3UriParts[3:], "/")
	bucket = s3UriParts[2]
	return
}

// GetBucket builds a s3 connection retrieving the bucket
func GetBucket(bucket string) *s3.Bucket {
	auth, err := awswrapper.GetAwsAuth()
	if err != nil {
		log.Fatalln(err)
	}
	b := s3.New(auth, aws.USEast).Bucket(bucket)
	loc, err := b.Location()
	if err != nil {
		log.Fatalln(err)

	}
	if aws.GetRegion(loc) != aws.USEast {
		b = s3.New(auth, aws.GetRegion(loc)).Bucket(bucket)
	}
	return b
}

// Ls lists directorys or keys under a prefix
func Ls(s3Uri string, searchDepth int, isRecursive, isHumanReadable, includeDate bool, logger *log.Logger) {
	bucket, prefix := parseS3Uri(s3Uri)
	b := GetBucket(bucket)

	var ch <-chan s3.Key
	ch = s3wrapper.FastList(b, prefix, searchDepth, isRecursive)

	for k := range ch {
		if k.Size < 0 {
			logger.Printf("%10s s3://%s/%s\n", "DIR", bucket, k.Key)
		} else {
			var size string
			if isHumanReadable {
				size = fmt.Sprintf("%10s", humanize.Bytes(uint64(k.Size)))
			} else {
				size = fmt.Sprintf("%10d", k.Size)
			}
			date := ""
			if includeDate {
				date = " " + k.LastModified
			}
			logger.Printf("%s%s s3://%s/%s\n", size, date, bucket, k.Key)
		}
	}
}

// Del deletes a set of prefixes(s3 keys or partial keys
func Del(prefixes []string, searchDepth int, isRecursive bool, logger *log.Logger) {
	if len(*delPrefixes) == 0 {
		logger.Println("No prefixes provided\n Usage: fasts3 del <prefix>")
		return
	}
	keys := make(chan string, len(prefixes)*2+1)
	var b *s3.Bucket = nil
	go func() {
		for _, delPrefix := range prefixes {
			bucket, prefix := parseS3Uri(delPrefix)

			if b == nil {
				b = GetBucket(bucket)
			}

			keys <- prefix
			if *delRecurse {
				keyExists, err := b.Exists(prefix)
				if err != nil {
					log.Fatalln(err)
				}

				if keyExists {
					keys <- prefix
				} else if *delRecurse {
					for key := range s3wrapper.FastList(b, prefix, searchDepth, true) {
						keys <- key.Key
					}

				} else {
					logger.Println("trying to delete a prefix, please add --recursive or -r to proceed")
				}
			}
		}
		close(keys)
	}()

	var wg sync.WaitGroup
	msgs := make(chan string, 1000)
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			batch := make([]string, 0, 100)
			for key := range keys {
				batch = append(batch, key)
				if len(batch) >= 100 {
					err := s3wrapper.DeleteMulti(b, batch)
					if err != nil {
						log.Fatalln(err)
					}
					for _, k := range batch {
						msgs <- fmt.Sprintf("File %s Deleted\n", k)
					}
					batch = batch[:0]
				}
			}

			if len(batch) > 0 {
				err := s3wrapper.DeleteMulti(b, batch)
				if err != nil {
					log.Fatalln(err)
				}
				for _, k := range batch {
					msgs <- fmt.Sprintf("File %s Deleted\n", k)
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(msgs)
	}()
	for msg := range msgs {
		logger.Print(msg)
	}
}

// initializes configs necessary for fasts3 utility
func Init(logger *log.Logger) error {
	usr, err := user.Current()
	if err != nil {
		return err
	}

	fs3cfg_path := path.Join(usr.HomeDir, ".fs3cfg")
	if _, err := os.Stat(fs3cfg_path); os.IsNotExist(err) {
		cfg := `[default]
access_key=<access_key>
secret_key=<secret_key>`
		ioutil.WriteFile(fs3cfg_path, []byte(cfg), 0644)
		logger.Printf("created template file %s\n", fs3cfg_path)
	} else {
		logger.Print(".fs3cfg already exists in home directory")
	}

	return nil
}

type GetRequest struct {
	Key            string
	OriginalPrefix string
}

// Get lists and retrieves s3 keys given a list of prefixes
// searchDepth can also be specified to increase speed of listing
func Get(prefixes []string, searchDepth int, keyRegex string, logger *log.Logger) {
	if len(prefixes) == 0 {
		logger.Println("No prefixes provided\n Usage: fasts3 get <prefix>")
		return
	}

	var keyRegexFilter *regexp.Regexp = getRegexOrNil(keyRegex)

	getRequests := make(chan GetRequest, len(prefixes)*2+1)
	var b *s3.Bucket = nil
	go func() {
		for _, prefix := range prefixes {
			_, actualPrefix := parseS3Uri(prefix)
			for key := range getKeysWithFilter(prefix, keyRegexFilter, searchDepth) {
				ogPrefix := actualPrefix
				if key.Key == prefix {
					keyParts := strings.Split(prefix, "/")
					ogPrefix = strings.Join(keyParts[0:len(keyParts)-1], "/") + "/"
				}

				getRequests <- GetRequest{Key: key.Key, OriginalPrefix: ogPrefix}
			}
		}
		close(getRequests)
	}()

	var wg sync.WaitGroup
	msgs := make(chan string, 1000)
	workingDirectory, err := os.Getwd()
	if err != nil {
		log.Fatalln(err)
	}
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			for rq := range getRequests {
				dest := path.Join(workingDirectory, strings.Replace(rq.Key, rq.OriginalPrefix, "", 1))
				msgs <- fmt.Sprintf("Getting %s -> %s\n", rq.Key, dest)
				err := s3wrapper.GetToFile(b, rq.Key, dest)
				if err != nil {
					log.Fatalln(err)
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(msgs)
	}()
	for msg := range msgs {
		logger.Print(msg)
	}
}

func getKeysWithFilter(prefix string, r *regexp.Regexp, searchDepth int) chan s3.Key {
	keys := make(chan s3.Key, 10000)
	go func() {
		defer close(keys)
		var b *s3.Bucket = nil
		bucket, prefix := parseS3Uri(prefix)

		if b == nil {
			b = GetBucket(bucket)
		}

		keyExists, err := b.Exists(prefix)
		if err != nil {
			log.Fatalln(err)
		}

		if keyExists && prefix != "" {
			if !isKeyMatch(prefix, r) {
				return
			}
			keys <- s3.Key{Key: prefix}
		} else {
			for key := range s3wrapper.FastList(b, prefix, searchDepth, true) {
				if !isKeyMatch(key.Key, r) {
					continue
				}
				keys <- key
			}

		}
	}()
	return keys
}

// Stream takes a set of prefixes lists them and
// streams the contents by line
func Stream(prefixes []string, searchDepth int, keyRegex string, includeKeyName bool, logger *log.Logger) {
	if len(prefixes) == 0 {
		logger.Println("No prefixes provided\n Usage: fasts3 get <prefix>\n")
		return
	}
	keys := make(chan string, len(prefixes)*2+1)
	var keyRegexFilter *regexp.Regexp = getRegexOrNil(keyRegex)

	var b *s3.Bucket = nil
	go func() {
		for _, prefix := range prefixes {
			for key := range getKeysWithFilter(prefix, keyRegexFilter, searchDepth) {
				keys <- key.Key
			}
		}
		close(keys)
	}()

	var wg sync.WaitGroup
	msgs := make(chan string, 1000)
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			for key := range keys {
				reader, err := s3wrapper.GetStream(b, key)
				if err != nil {
					panic(err)
				}
				for {
					line, _, err := reader.ReadLine()
					if err != nil {
						if err.Error() == "EOF" {
							break
						} else {
							log.Fatalln(err)
						}
					}
					msg := fmt.Sprintf("%s\n", string(line))
					if includeKeyName {
						msg = fmt.Sprintf("[%s] %s", key, msg)
					}
					msgs <- msg
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(msgs)
	}()
	for msg := range msgs {
		logger.Print(msg)
	}
}

// Copy copies src keys under the src prefix to the dest
func Copy(src string, dest string, keyRegex string, searchDepth int, logger *log.Logger) {
	var keyRegexFilter *regexp.Regexp = getRegexOrNil(keyRegex)

	copyRequests := make(chan GetRequest, 1000)

	srcBucket, srcPrefix := parseS3Uri(src)
	destBucket, destPrefix := parseS3Uri(dest)
	go func() {
		for key := range getKeysWithFilter(src, keyRegexFilter, searchDepth) {
			ogPrefix := srcPrefix
			if key.Key == src {
				keyParts := strings.Split(src, "/")
				ogPrefix = strings.Join(keyParts[0:len(keyParts)-1], "/") + "/"
			}
			copyRequests <- GetRequest{Key: key.Key, OriginalPrefix: ogPrefix}

		}
		close(copyRequests)
	}()

	var wg sync.WaitGroup
	msgs := make(chan string, 1000)
	b := GetBucket(destBucket)
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			for rq := range copyRequests {
				newDest := path.Join(destPrefix, strings.Replace(rq.Key, rq.OriginalPrefix, "", 1))
				msgs <- fmt.Sprintf("Copying s3://%s/%s -> s3://%s/%s\n", srcBucket, rq.Key, destBucket, newDest)
				copySrcKey := path.Join(srcBucket, rq.Key)
				err := s3wrapper.Copy(b, copySrcKey, newDest)
				if err != nil {
					log.Fatalln(err)
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(msgs)
	}()
	for msg := range msgs {
		logger.Print(msg)
	}
}

// getRegexOrNil
func getRegexOrNil(r string) *regexp.Regexp {
	if r != "" {
		return regexp.MustCompile(r)
	}
	return nil
}

func isKeyMatch(key string, r *regexp.Regexp) bool {
	return r == nil || r.MatchString(key)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	logBuf := bufio.NewWriter(os.Stdout)
	logger := log.New(logBuf, "", 0)
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case "ls":
		Ls(*lsS3Uri, *lsSearchDepth, *lsRecurse, *humanReadable, *withDate, logger)
	case "del":
		Del(*delPrefixes, *lsSearchDepth, *delRecurse, logger)
	case "get":
		Get(*getS3Uris, *getSearchDepth, *getKeyRegex, logger)
	case "stream":
		Stream(*streamS3Uris, *streamSearchDepth, *streamKeyRegex, *streamIncludeKeyName, logger)
	case "cp":
		Copy(*cpSrc, *cpDest, *cpKeyRegex, *cpSearchDepth, logger)
	case "init":
		Init(logger)
	}
	logBuf.Flush()
}

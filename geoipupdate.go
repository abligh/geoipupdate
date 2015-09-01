package main

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

var (
	sourceHost  = flag.String("source", "updates.maxmind.com", "source address for updates")
	protocol    = flag.String("protocol", "https", "protocol for updates (http or https)")
	directory   = flag.String("directory", "/usr/local/var/GeoIP", "directory to update")
	userId      = flag.String("userid", "999999", "MaxMind user ID")
	licenseKey  = flag.String("licensekey", "000000000000", "MaxMind licence Key")
	dolinks     = flag.Bool("links", true, "Create legacy symlinks")
	productIds  = flag.String("productids", "506,533,517", "Comma delimited product IDs")
	randomDelay = flag.String("randomdelay", "", "Wait for a random time period up to this amount")
)

var clientIp string

func isSuccess(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 209
}

func download(location string, query map[string]string) (*http.Response, []byte, error) {
	var vals url.Values = url.Values{}
	for k, v := range query {
		vals.Set(k, v)
	}
	u := url.URL{
		Host:   *sourceHost,
		Scheme: *protocol,
		Path:   location,
	}
	u.RawQuery = vals.Encode()
	res, err := http.Get(u.String())
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Printf("Download from %s ERROR %s", u.String(), err)
		return res, nil, err
	}
	return res, data, nil
}

func md5File(fn string) string {
	if data, err := ioutil.ReadFile(fn); err != nil {
		return "00000000000000000000000000000000"
	} else {
		hasher := md5.New()
		hasher.Write(data)
		return hex.EncodeToString(hasher.Sum(nil))
	}
}

func updateSecure(oldDigest string, productId string, challenge string) ([]byte, error) {
	response, data, err := download("/app/update_secure", map[string]string{
		"db_md5":        oldDigest,
		"challenge_md5": challenge,
		"user_id":       *userId,
		"edition_id":    productId,
	})
	if !isSuccess(response.StatusCode) {
		return nil, errors.New("Status " + response.Status + " received")
	}
	return data, err
}

func getProduct(productId string) error {
	if response, data, err := download("/app/update_getfilename", map[string]string{"product_id": productId}); err != nil {
		return err
	} else {
		if !isSuccess(response.StatusCode) {
			return errors.New("Status " + response.Status + " received")
		}
		filename := path.Base(string(data[:]))
		log.Printf("Attempting to update %s", filename)
		filePath := path.Join(*directory, filename)
		oldDigest := md5File(filePath)

		hasher := md5.New()
		hasher.Write([]byte(*licenseKey))
		hasher.Write([]byte(clientIp))
		challenge := hex.EncodeToString(hasher.Sum(nil))

		attempts := 0
		var uncompressed []byte
		for {
			if data, err := updateSecure(oldDigest, productId, challenge); err != nil {
				return err
			} else {
				if bytes.HasPrefix(data, []byte("No new updates available")) {
					if len(uncompressed) > 0 {
						log.Printf("Update retrieved for %s", filename)
						break
					} else {
						log.Printf("No new updates available for %s", filename)
						return nil
					}
				}
				if !bytes.HasPrefix(data, []byte("\x1f\x8b")) {
					return errors.New("Not a gzip file")
				}
				attempts++
				if attempts > 5 {
					return errors.New("Too many attempts at downloading file")
				}
				buf := bytes.NewBuffer(data)
				if gzr, err := gzip.NewReader(buf); err != nil {
					return err
				} else {
					defer gzr.Close()
					var err error
					if uncompressed, err = ioutil.ReadAll(gzr); err != nil {
						return err
					}
				}
				hasher := md5.New()
				hasher.Write(uncompressed)
				oldDigest = hex.EncodeToString(hasher.Sum(nil))
			}
		}

		tmpFilePath := filePath + ".tmp"
		if err := ioutil.WriteFile(tmpFilePath, uncompressed, 0644); err != nil {
			return err
		}
		if err := os.Rename(tmpFilePath, filePath); err != nil {
			return err
		}
	}

	return nil
}

func getClientIp() error {
	if response, data, err := download("/app/update_getipaddr", map[string]string{}); err != nil {
		return err
	} else {
		if !isSuccess(response.StatusCode) {
			return errors.New("Status " + response.Status + " received")
		}
		clientIp = string(data[:])
	}
	return nil
}

func randInt64(max int64) int64 {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("error:", err)
	}
	data := int64(binary.BigEndian.Uint64(b) & 0x7fffffffffffffff)
	return data % max
}

func main() {

	flag.Parse()
	if randomDelay != nil && *randomDelay != "" {
		if dur, err := time.ParseDuration(*randomDelay); err != nil {
			log.Fatalf("Cannot parse duration '%s': %v", randomDelay, err)
		} else {
			rdur := time.Duration(randInt64(dur.Nanoseconds()))
			log.Printf("Waiting for %s of %s", rdur.String(), dur.String())
			time.Sleep(rdur)
		}
	}

	log.Printf("Updating geoip database at %s from %s via %s", *directory, *sourceHost, *protocol)
	if err := getClientIp(); err != nil {
		log.Fatalf("Can't get client IP: %v", err)
	}
	for _, p := range strings.Split(*productIds, ",") {
		getProduct(p)
	}
	if *dolinks {
		log.Printf("Making legacy links in %s", *directory)
		os.Symlink(path.Join(*directory, "GeoLiteCity.dat"), path.Join(*directory, "GeoIPCity.dat"))
		os.Symlink(path.Join(*directory, "GeoLiteCountry.dat"), path.Join(*directory, "GeoIP.dat"))
	}
	log.Printf("Done\n")
}

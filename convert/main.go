package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

var (
	from     = flag.String("from", "/tmp/lb-log", "")
	to       = flag.String("to", "/tmp/lb-log-new", "")
	clear    = flag.Bool("clear", false, "")
	testLine = flag.Int("test-line", 1000, "")
)

func main() {
	flag.Parse()
	if *clear {
		if err := os.Remove(*to); err != nil {
			panic(err)
		}
	}
	// 2019-03-09T23:50:59.144177Z multimedia-proxy-lb 223.72.51.200:29190 10.190.249.220:80 0.000022 0.17247 0.000023 200 200 0 16275 "GET https://cloud-cdn.tantanapp.com:443/v1/images/eyJpZCI6IlZLNFhFQVNZQ08zS1JaQUtLSlZDUUxPR09GQU1BTiIsInciOjkzMSwiaCI6OTMxLCJkIjowLCJtdCI6ImltYWdlL2pwZWciLCJkaCI6MTMyMjEyNjM2MTAzMTQzNzA1MTAsImFiIjowfQ.jpg?format=480x480 HTTP/1.1" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.324213Z multimedia-proxy-lb 183.134.9.22:64588 10.190.255.22:80 0.000026 0.000393 0.000024 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.331016Z multimedia-proxy-lb 183.216.176.90:52904 10.190.249.144:80 0.000026 0.000407 0.000024 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.342935Z multimedia-proxy-lb 124.225.168.51:46636 10.190.255.22:80 0.000024 0.000405 0.000024 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.343502Z multimedia-proxy-lb 106.120.178.23:47691 10.190.249.144:80 0.000023 0.000468 0.000026 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.353701Z multimedia-proxy-lb 123.128.14.179:55886 10.190.255.22:80 0.000023 0.000396 0.000023 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.334127Z multimedia-proxy-lb 113.7.18.98:53523 10.190.249.220:80 0.000024 0.034196 0.000024 200 200 0 13043 "GET https://cloud-cdn.tantanapp.com:443/v1/images/eyJpZCI6IlhDVlhQNDZCQVZIT0lSSFFNR1pOVVM1VExaTURNVyIsInciOjk4NCwiaCI6OTYxLCJkIjowLCJtdCI6ImltYWdlL2pwZWciLCJkaCI6MTAwMjcxMDUwNDgxMjg3MzA0MjQsImFiIjowfQ.jpg?format=480x480 HTTP/1.1" "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.369625Z multimedia-proxy-lb 113.59.38.17:57444 10.190.249.144:80 0.000026 0.000541 0.000024 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.248593Z multimedia-proxy-lb 117.136.0.165:41414 10.190.249.220:80 0.000026 0.125169 0.000022 200 200 16439 303 "POST https://cloud.tantanapp.com:443/v1/upload/audio HTTP/1.1" "Putong/3.4.1 Android/25 OPPO/OPPO+A73" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
	// 2019-03-09T23:50:59.375420Z multimedia-proxy-lb 121.22.247.208:45206 10.190.255.22:80 0.000024 0.000436 0.000023 200 200 0 22272 "GET https://cloud-cdn.tantanapp.com:443/do_not_delete/noc.gif HTTP/1.1" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36 JianKongBao Monitor 1.1" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2

	// 2019-03-08T23:55:31.319991Z images eyJpZCI6IkZTRk9FRkVKSVZRRTNGVFREVVlVNlVDNVBRTDVEWCIsInciOjk2MCwiaCI6OTYwLCJkIjowLCJtdCI6ImltYWdlL2pwZWciLCJkaCI6NzY5ODU3NjI0NjA4ODI5MjMxM30
	// 2019-03-08T23:55:31.321042Z images eyJpZCI6IldMTEJZNlU2QlZFWExUN1czSDY3N1ZDNlZNQlRZUCIsInciOjcyMCwiaCI6NzIwLCJkIjowLCJtdCI6ImltYWdlL2pwZWciLCJkaCI6MTUwMzQ4ODc1ODg3MDcyNDgwNzcsImFiIjowfQ
	// 2019-03-08T23:55:31.320376Z images eyJpZCI6Ik5UV0ZOSVgzT1BNU1dJN09FSFNRVUVFNUYzWldXRiIsInciOjE0NDAsImgiOjE0NDAsImQiOjAsIm10IjoiaW1hZ2UvanBlZyIsImRoIjo1MjgxMDA2NjYwMzMyNzcwMDY3LCJhYiI6MH0
	// 2019-03-08T23:55:31.304268Z images images
	f1, err := os.Open(*from)
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	f2, err := os.OpenFile(*to, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	scanner := bufio.NewScanner(f1)
	counter := 0
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		if len(fields) < 3 {
			fmt.Println("not good", fields)
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, fields[0])
		if err != nil {
			fmt.Println("cannot parse time", err, fields[0])
		}
		fields[0] = fmt.Sprint(t.Unix())

		key := fields[2]
		bs, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(key))
		if err != nil {
			fmt.Println("failed to base 64 decode", err, key)
			goto final
		}
		if id := fastjson.GetString(bs, "id"); id != "" {
			key = id
		}
	final:
		fields[2] = key
		// fmt.Println(fields)
		_, err = f2.WriteString(strings.Join(fields, " ") + "\n")
		if err != nil {
			panic(err)
		}
		counter++
		if *testLine > 0 && counter > *testLine {
			break
		}
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	pb "github.com/soslanco/ggmd/proto"
	"google.golang.org/grpc"
)

var (
	fgRPC      = flag.String("grpc", "127.0.0.1:9090", "gRPC server address")
	fURL       = flag.String("fetch", "", "URL of CSV data file.")
	flStartId  = flag.Uint64("lid", 0, "List first item id. Indexing starts at 0. (default 0)")
	flPageSize = flag.Uint64("lsize", 10, "List page size.")
	flSort     = flag.String("lsort", "name", "List sort order: <name|count|price|date>.")
	flSortDir  = flag.Int64("lsortdir", 1, "Sort direction: 1 (ASC), -1 (DESC).")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*fgRPC, []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}...)
	if err != nil {
		log.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewMarketClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if *fURL != "" {
		// Fetch
		log.Printf("Fetching %s...", *fURL)
		req := &pb.FetchRequest{Url: *fURL}
		resp, err := client.Fetch(ctx, req)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Printf("Result: %s", resp.GetStatus())
		}
	} else {
		// List
		if *flSort != "name" && *flSort != "count" && *flSort != "price" && *flSort != "date" || *flPageSize == 0 || (*flSortDir != 1 && *flSortDir != -1) {
			log.Fatalln("unsupported lsort parameter value.")
		}

		fmt.Printf("Listing (start_id: %d, page_size: %d, sort by: \"%s\", sort dir: %d)...\n", *flStartId, *flPageSize, *flSort, *flSortDir)
		req := &pb.ListRequest{
			StartId:  int64(*flStartId),
			PageSize: uint32(*flPageSize),
			Sort:     pb.ListRequest_SortField(pb.ListRequest_SortField_value[strings.ToUpper(*flSort)]),
			SortDir:  int64(*flSortDir),
		}
		resp, err := client.List(ctx, req)
		if err != nil {
			log.Fatalln(err)
		} else {
			for _, item := range resp.List {
				fmt.Printf("%-20s %6d %15f  %s\n", item.Name, item.Count, item.Price, item.Date.AsTime())
			}
			fmt.Printf("Next ID: %v\n", resp.NextId)
		}
	}
}

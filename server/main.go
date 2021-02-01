package main

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/soslanco/ggmd/proto"
	"google.golang.org/grpc"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	//"go.mongodb.org/mongo-driver/mongo/readconcern"
	//"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type MarketServer struct {
	pb.UnimplementedMarketServer
	srv *Srv
}

// Fetch
func (ms *MarketServer) Fetch(ctx context.Context, in *pb.FetchRequest) (*pb.FetchResponse, error) {
	select {
	case <-ctx.Done():
		ms.srv.logger.Println(ctx.Err())
		return &pb.FetchResponse{Status: pb.FetchResponse_StatusCode(pb.FetchResponse_StatusCode_value["ERROR"])}, ctx.Err()
	case ms.srv.task <- in.GetUrl():
		return &pb.FetchResponse{Status: pb.FetchResponse_StatusCode(pb.FetchResponse_StatusCode_value["QUEUED"])}, nil
	}
}

// List
func (ms *MarketServer) List(ctx context.Context, in *pb.ListRequest) (*pb.ListResponse, error) {

	mcol := ms.srv.db.Client.Database(ms.srv.dbName).Collection(ms.srv.colMarketName)

	var sfields bson.D

	if str := strings.ToLower(in.GetSort().String()); str == "name" {
		sfields = bson.D{{"name", in.GetSortDir()}}
	} else if str == "count" {
		sfields = bson.D{{"count", in.GetSortDir()}, {"name", 1}}
	} else if str == "price" {
		sfields = bson.D{{"price", in.GetSortDir()}, {"name", 1}}
	} else if str == "date" {
		sfields = bson.D{{"date", in.GetSortDir()}, {"name", 1}}
	} else {
		sfields = bson.D{{"name", 1}}
	}

	sort_stage := bson.D{{"$sort", sfields}}
	skip_stage := bson.D{{"$skip", in.GetStartId()}}
	limit_stage := bson.D{{"$limit", in.GetPageSize() + 1}}
	project_stage := bson.D{{"$project", bson.M{"_id": 0, "name": 1, "count": 1, "price": 1, "date": 1}}}

	cursor, err := mcol.Aggregate(ctx, mongo.Pipeline{sort_stage, skip_stage, limit_stage, project_stage})
	if err != nil {
		ms.srv.logger.Println(err)
		return &pb.ListResponse{}, err
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		ms.srv.logger.Println(err)
		return &pb.ListResponse{}, err
	}

	var nextId int64
	if len(results) == int(in.GetPageSize()+1) {
		// Next item exist
		nextId = in.GetStartId() + int64(in.GetPageSize())
		results = results[:in.GetPageSize()]
	} else {
		// Last page
		nextId = -1
	}

	resp := &pb.ListResponse{
		List:   make([]*pb.ListResponse_Item, len(results)),
		NextId: nextId,
	}

	for i, result := range results {
		resp.List[i] = &pb.ListResponse_Item{
			Name:  result["name"].(string),
			Count: result["count"].(int64),
			Price: result["price"].(float64),
			//Date:  timestamppb.New(time.Unix(result["date"].(int64)/1000, result["date"].(int64)%1000*1000000)),

			// https://pkg.go.dev/google.golang.org/protobuf/types/known/timestamppb
			//Date:  timestamppb.Now(),
			//Date:  timestamppb.New(time.Now()),
			Date: timestamppb.New(result["date"].(primitive.DateTime).Time()),
		}
	}
	return resp, nil
}

type Pair struct {
	price float64
	count int64
}

type Srv struct {
	db            *MongoDB
	dbName        string
	colTaskName   string
	colMarketName string
	task          chan string
	done          chan struct{}
	logger        *log.Logger
}

// NewSrv
func NewSrv(mongodb string) *Srv {
	return &Srv{
		dbName:        mongodb,
		colTaskName:   "task",
		colMarketName: "market",
		task:          make(chan string, 1),
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile),
	}
}

// Update DB
func (srv *Srv) Update(url string) (err error) {
	var res *mongo.InsertOneResult

	now := time.Now().UTC()

	tcol := srv.db.Client.Database(srv.dbName).Collection(srv.colTaskName)
	mcol := srv.db.Client.Database(srv.dbName).Collection(srv.colMarketName)

	// Check URL status
	var doc bson.M
	err = tcol.FindOne(context.TODO(), bson.M{"url": url}).Decode(&doc)
	if err != mongo.ErrNoDocuments {
		if err != nil {
			return fmt.Errorf("Skip processing %s. %s", url, err)
		}
		return fmt.Errorf("Skip processing %s. Status: %s", url, doc["status"])
	}

	res, err = tcol.InsertOne(context.TODO(), bson.M{"url": url, "status": "pending", "date": now})
	if err != nil {
		return fmt.Errorf("Skip processing %s. %s", url, err)
	}

	id := res.InsertedID

	status := "failure"
	defer func() {
		tcol.UpdateOne(context.TODO(), bson.D{{"_id", id}}, bson.D{{"$set", bson.M{"status": status}}})
	}()

	// GET CSV file

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	// ignore server SSL certificate
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}
	// Close idle connection
	// (either can be released through client transport)
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Preproccess data
	var data map[string]*Pair = make(map[string]*Pair)
	r := csv.NewReader(resp.Body)
	r.Comma = ';'         // use ';' field separator instead default one ','
	r.FieldsPerRecord = 2 // required number of fields in each row
	r.ReuseRecord = true  // record value valid until next Read call
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// process only valid values
		if price, err := strconv.ParseFloat(rec[1], 64); err == nil && price > 0 {
			if d, ok := data[rec[0]]; ok {
				d.price = price
				d.count += 1
			} else {
				data[rec[0]] = &Pair{price, 1}
			}
		}
	}

	// Insert/Update market data in trasaction
	var count int
	/*
		if len(data) > 0 {
			wc := writeconcern.New(writeconcern.WMajority())
			rc := readconcern.Snapshot()
			txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)
			sess, err := srv.db.Client.StartSession()
			if err != nil {
				return err
			}
			defer sess.EndSession(context.TODO())

			sess.WithTransaction(context.TODO(), func(sCtx mongo.SessionContext) (interface{}, error) {
	*/
	opts := options.FindOneAndUpdate().SetUpsert(true)
	for k, v := range data {
		filter := bson.M{"name": k}
		update := bson.D{
			{"$set", bson.D{{"price", v.price}, {"date", now}}},
			{"$inc", bson.D{{"count", v.count}}},
		}
		//sr := mcol.FindOneAndUpdate(sCtx, filter, update, opts)
		sr := mcol.FindOneAndUpdate(context.TODO(), filter, update, opts)
		if sr.Err() != nil && sr.Err() != mongo.ErrNoDocuments {
			// partial update allowed
			srv.logger.Println(sr.Err())
		} else {
			count++
		}
	}
	/*
				if count == 0 {
					return nil, fmt.Errorf("zero updates")
				}
				return nil, err
			}, txnOpts)
		}
	*/

	// Update task status
	srv.logger.Printf("Inserted/Updated %d/%d records", count, len(data))
	if count > 0 || len(data) == 0 {
		status = "success"
	} else {
		err = fmt.Errorf("Failure")
	}

	return err
}

// fetcher goroutine manage database updating
func (srv *Srv) fetcher(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-srv.done:
			srv.logger.Print("Fetcher stoped")
			return
		case url := <-srv.task:
			srv.logger.Printf("Processing %s...", url)
			err := srv.Update(url)
			if err != nil {
				srv.logger.Printf("Update error: %s", err)
			} else {
				srv.logger.Println("DB successfully updated")
			}
		}
	}

}

// ConnectDB
func (srv *Srv) ConnectDB(url string) (err error) {
	dbc, err := NewMongoDB(url)
	if err == nil {
		srv.db = dbc
	}
	return
}

// CloseDB
func (srv *Srv) CloseDB() error {
	return srv.db.Close()
}

// CheckInitDB
func (srv *Srv) CheckInitDB() error {

	filter := bson.M{"name": bson.M{"$in": bson.A{srv.colTaskName, srv.colMarketName}}}
	names, err := srv.db.Client.Database(srv.dbName).ListCollectionNames(context.TODO(), filter)
	if err != nil {
		return err
	}

	if len(names) == 0 {
		// Create collection UPDATE
		srv.logger.Printf("Creating task (%s.%s) collection...", srv.dbName, srv.colTaskName)
		err = createStatusCollection(srv.db.Client.Database(srv.dbName), srv.colTaskName)
		if err != nil {
			return err
		}
		srv.logger.Println("Collection successfully created")

		// Create collection MARKET
		srv.logger.Printf("Creating market (%s.%s) collection...", srv.dbName, srv.colMarketName)
		err = createMarketCollection(srv.db.Client.Database(srv.dbName), srv.colMarketName)
		if err != nil {
			return err
		}
		srv.logger.Println("Collection successfully created")
	}

	return nil
}

// createStatusCollection
func createStatusCollection(db *mongo.Database, colname string) error {
	jsonSchema := bson.M{
		"bsonType": "object",
		"required": []string{"url", "status", "date"},
		"properties": bson.M{
			"url":    bson.M{"bsonType": "string"},
			"status": bson.M{"bsonType": "string", "enum": bson.A{"pending", "success", "failure"}},
			"date":   bson.M{"bsonType": "date"},
		},
	}
	opts := options.CreateCollection().SetValidator(bson.M{"$jsonSchema": jsonSchema})
	err := db.CreateCollection(context.TODO(), colname, opts)
	if err != nil {
		return err
	}

	indexView := db.Collection(colname).Indexes()

	// Create indicies
	models := []mongo.IndexModel{{Keys: bson.D{{"url", 1}}, Options: options.Index().SetUnique(true)}}
	_, err = indexView.CreateMany(context.TODO(), models, nil)
	if err != nil {
		return err
	}

	return nil
}

// createMarketCollection
func createMarketCollection(db *mongo.Database, colname string) error {
	jsonSchema := bson.M{
		"bsonType": "object",
		"required": []string{"name", "count", "price", "date"},
		"properties": bson.M{
			"name":  bson.M{"bsonType": "string"},
			"count": bson.M{"bsonType": "long", "minimum": 1},
			"price": bson.M{"bsonType": "double", "minimum": 0, "exclusiveMinimum": true},
			"date":  bson.M{"bsonType": "date"},
		},
	}
	opts := options.CreateCollection().SetValidator(bson.M{"$jsonSchema": jsonSchema})
	err := db.CreateCollection(context.TODO(), colname, opts)
	if err != nil {
		return err
	}

	indexView := db.Collection(colname).Indexes()

	// Create indicies
	models := []mongo.IndexModel{
		{Keys: bson.D{{"name", 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{"count", 1}}},
		{Keys: bson.D{{"price", 1}}},
		{Keys: bson.D{{"date", 1}}},
	}

	_, err = indexView.CreateMany(context.TODO(), models, nil)
	if err != nil {
		return err
	}
	return nil
}

// main
func main() {
	var wg sync.WaitGroup

	eMongoURI := os.Getenv("GGMD_MOGO_URI")
	if eMongoURI == "" {
		eMongoURI = "mongodb://root:example@mongo2"
	}

	eMongoDB := os.Getenv("GGMD_MOGO_DB")
	if eMongoDB == "" {
		eMongoDB = "crypto"
	}

	eRPCAddr := os.Getenv("GGMD_RPC_ADDR")
	if eRPCAddr == "" {
		eRPCAddr = ":9090"
	}

	srv := NewSrv(eMongoDB)

	// Connecting to DB
	srv.logger.Println("Connecting to DB...")
	err := srv.ConnectDB(eMongoURI)
	if err != nil {
		srv.logger.Fatalln(err)
	}
	// Ping DB connection
	err = srv.db.Ping()
	if err != nil {
		srv.logger.Fatalln(err)
	}
	srv.logger.Println("Connection with DB successfully established")

	// Init DB (for testing purpose only)
	err = srv.CheckInitDB()
	if err != nil {
		srv.logger.Println("DB initialization failed. Go forward.")
	}

	wg.Add(1)
	go srv.fetcher(&wg)

	// gRPC Listening
	lst, err := net.Listen("tcp", eRPCAddr)
	if err != nil {
		srv.logger.Fatalf("failed to listen grpc: %v", err)
	}

	// Start gRPC server
	ms := grpc.NewServer()
	pb.RegisterMarketServer(ms, &MarketServer{srv: srv})
	if err := ms.Serve(lst); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	/*
		// Test data (back order)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2018.csv"
		time.Sleep(5)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2017.csv"
		time.Sleep(5)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2016.csv"
		time.Sleep(5)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2015.csv"
		time.Sleep(5)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2014.csv"
		time.Sleep(5)
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2013.csv"
		time.Sleep(5)

		// Fail test
		srv.task <- "https://storage.googleapis.com/soslanco/crypto-2013.csv"
		srv.task <- "https://net.takogo.adresa.ru/1.csv"
	*/

	// Handle OS Signals
	osSignal := make(chan os.Signal, 10)
	signal.Notify(osSignal, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	sig := <-osSignal
	srv.logger.Printf("Server received signal: %s (%#v)", sig, sig)

	// EXIT

	close(srv.done)

	// Waiting for goroutines to complete successfully
	wg.Wait()

	// Close DB connection
	srv.logger.Println("Closing DB connection...")
	err = srv.CloseDB()
	if err != nil {
		srv.logger.Println(err)
	} else {
		srv.logger.Println("DB connection closed")
	}

	srv.logger.Print("Server stoped")
	os.Exit(0)
}

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/beego/beego/v2/core/logs"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// some params for mongo
const (
	user     = "root"
	password = "root"
	hosts    = "127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019"
	mongoOpt = "replicaSet=rs0"
	auth     = "admin"
	timeout  = time.Duration(3000) * time.Millisecond
)

// a struct mapping mongo data
type student struct {
	Name   string `bson:"name"`
	Gender string `bson:"gender"`
	Age    int    `bson:"age"`
}

func main() {
	uri := fmt.Sprintf("mongodb://%s:%s@%s/%s?%s",
		user, password, hosts, auth, mongoOpt)
	opt := options.Client().
		ApplyURI(uri).
		SetSocketTimeout(timeout).
		SetReadPreference(readpref.SecondaryPreferred()) // Read-write separation, read slave library first

	// generate a context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// get a mongo client
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		logs.Error("connect mongo failed, err:%s", err.Error())
		return
	}

	// ping mongo to valid this mongo connection
	err = client.Ping(ctx, nil)
	if err != nil {
		logs.Error("ping mongo failed, err:%s", err.Error())
		return
	}

	database := "school"
	collection := "student"
	// some data will insert
	students := []interface{}{
		student{
			Name:   "Michael",
			Gender: "Male",
			Age:    21,
		},
		student{
			Name:   "Alice",
			Gender: "Female",
			Age:    19,
		},
	}

	// insert data to mongo with transaction
	if err = client.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		// start transaction
		if err := sessionContext.StartTransaction(); err != nil {
			return err
		}
		// close transaction before this function return
		defer sessionContext.EndSession(ctx)

		// batch insert data to mongo
		if _, err := client.Database(database).Collection(collection).InsertMany(ctx, students); err != nil {
			if err := sessionContext.AbortTransaction(context.Background()); err != nil {
				// if it has some error, we need rollback
				logs.Error("mongo transaction rollback failed, %s", err.Error())
				return err
			}
			return err
		}
		// not error, we should commit this transaction
		return sessionContext.CommitTransaction(context.Background())
	}); err != nil {
		logs.Error("insert failed, err:%s", err.Error())
	}
}

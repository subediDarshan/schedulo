package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const DefaultHeartbeatInterval = 5 * time.Second

func GetDBConnectionString() string {
	var missingEnv []string

	checkEnvVar := func(envVar, envVarName string) {
		if envVar == "" {
			missingEnv = append(missingEnv, envVarName)
		}
	}

	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")

	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")

	if len(missingEnv) > 0 {
		log.Fatalf("Missing env variables: %s", strings.Join(missingEnv, ", "))
	}

	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
}

func ConnectToDB(ctx context.Context, dbConnectionString string) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error

	retryCount := 0

	for retryCount < 5 {
		dbPool, err = pgxpool.Connect(ctx, dbConnectionString)
		if err == nil {
			break
		}

		log.Print("Failed to connect to db. Retrying in 5 seconds...", err)

		time.Sleep(5 * time.Second)

		retryCount++

	}

	if err != nil {
		log.Print("Error connecting to db")
		return nil, err
	}

	log.Print("Connected to Database")

	return dbPool, nil

}

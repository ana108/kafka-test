package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	var msg string
	// google platform googlecloudplatform.lst
	allHosts, err := readLines("awsplatform.lst")
	if err != nil {
		panic(err)
	}
	openDb()
	fmt.Println("Start Time: ", time.Now().Format(time.RFC850))
	for i, host := range allHosts {
		if i < 5 {
			fmt.Println("Continuing...")
			continue
		}
		fmt.Println(host)
		fmt.Println("Plaintext")
		_, msg = plainText(host)
		processResults(host, "plaintext", msg, "awsplatform")
		fmt.Println("SASL-Plaintext")
		_, msg = saslPlain(host)
		processResults(host, "sasl_plaintext", msg, "awsplatform")
		fmt.Println("SASL-SSL")
		_, msg = saslTLS(host)
		processResults(host, "sasl_ssl", msg, "awsplatform")
		fmt.Println("TLS")
		_, msg = tlsClient(host)
		processResults(host, "tls", msg, "awsplatform")
	}
	fmt.Println("End Time: ", time.Now().Format(time.RFC850))
	closeDb()
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func processResults(host string, securityType string, msg string, tags string) {
	if strings.Contains(msg, "ConnectionSuccessStart") {
		msg = strings.ReplaceAll(msg, "ConnectionSuccessStart:", "")
		msg = strings.ReplaceAll(msg, "ConnectionSuccessEnd", "")
		saveResults(host, securityType, msg, "", tags)
	} else {
		msg = strings.ReplaceAll(msg, "ConsumerStart:", "")
		msg = strings.ReplaceAll(msg, "ConsumerEnd", "")
		saveResults(host, securityType, "", msg, tags)
	}
	fmt.Println(msg)
}
func plainText(broker string) (string, string) {
	config := sarama.NewConfig()
	config.ClientID = "internetmeasurements"
	config.Consumer.Return.Errors = true

	//kafka end point
	brokers := []string{broker}

	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags)

	sarama.Logger = logger
	//get broker
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return broker, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
	}

	defer func() {
		if err := cluster.Close(); err != nil {
			panic(err)
		}
	}()

	//get all topic from cluster
	topics, _ := cluster.Topics()
	return broker, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessEnd", topics)
}

func tlsClient(broker string) (string, string) {
	tlsConfig, err := NewTLSConfig("clientcerts/client.cer.pem",
		"clientcerts/client.key.pem",
		"clientcerts/root.crt")
	if err != nil {
		fmt.Println("Could not open the client certs!!")
		log.Fatal(err)
	}
	tlsConfig.InsecureSkipVerify = true

	config := sarama.NewConfig()
	config.ClientID = "internetmeasurements"
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Consumer.Return.Errors = true

	//kafka end point
	brokers := []string{broker}

	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags)

	sarama.Logger = logger
	//get broker
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return broker, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
	}

	defer func() {
		if err := cluster.Close(); err != nil {
			panic(err)
		}
	}()

	//get all topic from cluster
	topics, _ := cluster.Topics()
	return broker, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
}

func saslPlain(broker string) (string, string) {
	config := sarama.NewConfig()
	config.ClientID = "internetmeasurements"
	config.Consumer.Return.Errors = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "admin"
	config.Net.SASL.Password = "admin-secret"
	config.Net.SASL.Handshake = true
	//kafka end point
	brokers := []string{broker}

	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags)

	sarama.Logger = logger
	//get broker
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return broker, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
	}

	defer func() {
		if err := cluster.Close(); err != nil {
			panic(err)
			//return host, port, fmt.Sprintf("DataReadErrorStart:%sDataReadErrorEnd", &buf)
		}
	}()

	//get all topic from cluster
	topics, _ := cluster.Topics()
	return broker, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
}
func saslTLS(broker string) (string, string) {

	tlsConfig, err := NewTLSConfig("clientcerts/client.cer.pem",
		"clientcerts/client.key.pem",
		"clientcerts/root.crt")
	if err != nil {
		fmt.Println("Could not open the client certs!!")
		log.Fatal(err)
	}
	tlsConfig.InsecureSkipVerify = true

	config := sarama.NewConfig()
	config.Net.TLS.Enable = true
	config.Net.TLS.Config = tlsConfig
	config.Consumer.Return.Errors = true
	config.ClientID = "internetmeasurements"
	config.Consumer.Return.Errors = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "admin"
	config.Net.SASL.Password = "admin-secret"
	config.Net.SASL.Handshake = true
	//kafka end point
	brokers := []string{broker}

	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags)

	sarama.Logger = logger
	//get broker
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return broker, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
	}

	defer func() {
		if err := cluster.Close(); err != nil {
			panic(err)
			//return host, port, fmt.Sprintf("DataReadErrorStart:%sDataReadErrorEnd", &buf)
		}
	}()

	//get all topic from cluster
	topics, _ := cluster.Topics()
	return broker, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
}

const (
	host     = os.Getenv("PGHOST")
	port     = 25060
	user     = os.Getenv("PGUSER")
	password = os.Getenv("PGPASSWORD")
	dbname   = "defaultdb"
)

var db *sql.DB
var err error

func openDb() {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", host, port, user, password, dbname)

	// open database
	db, err = sql.Open("postgres", psqlconn)
	CheckError(err)

	// check db
	err = db.Ping()
	CheckError(err)

	fmt.Println("Connected!")
}
func closeDb() {
	fmt.Println("Closed DB")
	// close database
	defer db.Close()
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

func saveResults(broker string, security_method string, success string, errors string, tags string) {
	insertDynStmt := `insert into "results"("broker", "security_method", "success", "errors", "tags") values($1, $2, $3, $4, $5)`
	_, e := db.Exec(insertDynStmt, broker, security_method, success, errors, tags)
	if e != nil {
		fmt.Println("Error occurred writing to db: ", e)
	}
	// CheckError(e)
}

// func plainText(host string, port string) (string, string, string) {
// 	config := sarama.NewConfig()
// 	config.Consumer.Return.Errors = true

// 	//kafka end point
// 	brokers := []string{fmt.Sprintf("%s:%s", host, port)}

// 	var buf bytes.Buffer
// 	logger := log.New(&buf, "", log.LstdFlags)

// 	sarama.Logger = logger
// 	//get broker
// 	cluster, err := sarama.NewConsumer(brokers, config)
// 	if err != nil {
// 		return host, port, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
// 	}

// 	defer func() {
// 		if err := cluster.Close(); err != nil {
// 			panic(err)
// 			//return host, port, fmt.Sprintf("DataReadErrorStart:%sDataReadErrorEnd", &buf)
// 		}
// 	}()

// 	//get all topic from cluster
// 	topics, _ := cluster.Topics()
// 	// for index := range topics {
// 	// 	fmt.Println(topics[index])
// 	// }
// 	return host, port, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
// }

// func tlsClient(host string, port string) (string, string, string) {
// 	tlsConfig, err := NewTLSConfig("clientcerts/client.cer.pem",
// 		"clientcerts/client.key.pem",
// 		"clientcerts/root.crt")
// 	if err != nil {
// 		fmt.Println("Could not open the client certs!!")
// 		log.Fatal(err)
// 	}
// 	tlsConfig.InsecureSkipVerify = true

// 	config := sarama.NewConfig()
// 	config.Net.TLS.Enable = true
// 	config.Net.TLS.Config = tlsConfig
// 	config.Consumer.Return.Errors = true

// 	//kafka end point
// 	brokers := []string{fmt.Sprintf("%s:%s", host, port)}

// 	var buf bytes.Buffer
// 	logger := log.New(&buf, "", log.LstdFlags)

// 	sarama.Logger = logger
// 	//get broker
// 	cluster, err := sarama.NewConsumer(brokers, config)
// 	if err != nil {
// 		return host, port, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
// 	}

// 	defer func() {
// 		if err := cluster.Close(); err != nil {
// 			panic(err)
// 			//return host, port, fmt.Sprintf("DataReadErrorStart:%sDataReadErrorEnd", &buf)
// 		}
// 	}()

// 	//get all topic from cluster
// 	topics, _ := cluster.Topics()
// 	// for index := range topics {
// 	// 	fmt.Println(topics[index])
// 	// }
// 	return host, port, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
// }

// func saslPlain(host string, port string) (string, string, string) {
// 	config := sarama.NewConfig()

// 	config.Consumer.Return.Errors = true
// 	config.Net.SASL.Enable = true
// 	config.Net.SASL.User = "admin"
// 	config.Net.SASL.Password = "admin-secret1"
// 	config.Net.SASL.Handshake = true
// 	//kafka end point
// 	brokers := []string{fmt.Sprintf("%s:%s", host, port)}

// 	var buf bytes.Buffer
// 	logger := log.New(&buf, "", log.LstdFlags)

// 	sarama.Logger = logger
// 	//get broker
// 	cluster, err := sarama.NewConsumer(brokers, config)
// 	if err != nil {
// 		return host, port, fmt.Sprintf("ConsumerStart:%sConsumerEnd", &buf)
// 	}

// 	defer func() {
// 		if err := cluster.Close(); err != nil {
// 			panic(err)
// 			//return host, port, fmt.Sprintf("DataReadErrorStart:%sDataReadErrorEnd", &buf)
// 		}
// 	}()

// 	//get all topic from cluster
// 	topics, _ := cluster.Topics()
// 	return host, port, fmt.Sprintf("ConnectionSuccessStart:%sConnectionSuccessStart", topics)
// }

func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

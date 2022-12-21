package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	// gatherHosts("")
	startCollecting()
}

var wg sync.WaitGroup

func gatherHosts(nextToken string) {
	client := http.Client{}
	url := "https://search.censys.io/api/v2/hosts/search?q=services.service_name%3A%20KAFKA%20and%20NOT%20autonomous_system.name%20%3D%20%60AMAZON-02%60%20and%20NOT%20autonomous_system.name%3D%60GOOGLE-CLOUD-PLATFORM%60&per_page=100&virtual_hosts=EXCLUDE"

	fmt.Println("Next Token Receive is ", nextToken)
	if nextToken != "" {
		url = url + "&cursor=" + nextToken
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Error received ", err)
	}

	authorizationHeader := "Basic " + os.Getenv("BASIC_AUTH")
	req.Header = http.Header{
		"Host":          {"www.host.com"},
		"Content-Type":  {"application/json"},
		"Authorization": {authorizationHeader},
	}

	res, err := client.Do(req)
	if err != nil {
		fmt.Println("ERror while calling url ", err)
	}
	body, errParse := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if errParse != nil {
		fmt.Println("Parsing error", errParse)
	}

	var x map[string]interface{}

	json.Unmarshal([]byte(string(body)), &x)

	rawResult := x["result"]
	if rawResult != nil {
		rawResult = x["result"].(map[string]interface{})
	} else {
		fmt.Println("Error!!! ", x)
	}
	result := x["result"].(map[string]interface{})

	hits := x["result"].(map[string]interface{})["hits"].([]interface{})
	var s []string
	for _, host := range hits {
		ipAddr := host.(map[string]interface{})["ip"]
		services := host.(map[string]interface{})["services"].([]interface{})
		for _, service := range services {
			serviceName := service.(map[string]interface{})["service_name"]
			servicePort := service.(map[string]interface{})["port"]
			if serviceName == "KAFKA" {
				fullBrokerName := fmt.Sprintf("%s:%d", ipAddr, int(servicePort.(float64)))
				s = append(s, fullBrokerName)
				break
			}
		}
	}
	saveAllToFile(s, "regular")
	getNextToken := result["links"].(map[string]interface{})["next"].(string)
	if getNextToken != "" {
		gatherHosts(getNextToken)
	}
}
func saveAllToFile(brokers []string, filename string) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()
	brokersInString := strings.Join([]string(brokers), "\n")
	if _, err = f.WriteString(brokersInString); err != nil {
		panic(err)
	}
}

func startCollecting() {
	openDb()
	// hostLists := [3]string{"regular", "awsplatform", "gcp"}
	hostLists := [1]string{"regular"}
	startIndex := 0
	endIndex := 7101
	timeToSleep := 0
	for _, src := range hostLists {
		allHosts, err := readLines(src)
		if err != nil {
			panic(err)
		}
		for idx, host := range allHosts {
			if idx < startIndex {
				continue
			}
			if idx >= endIndex {
				break
			}
			timeToSleep = timeToSleep + 1
			if timeToSleep > 1000 {
				timeToSleep = 0
				fmt.Println("Sleep minute start")
				time.Sleep(60 * time.Second)
				fmt.Println("Sleep minute end")
			}
			wg.Add(1)
			go processByHost(host, src)
		}
	}
	wg.Wait()
	defer closeDb()
}

func processByHost(host string, tag string) {
	defer wg.Done()
	var msg string
	fmt.Println(host, "Start Time: ", time.Now().Format(time.RFC850))
	fmt.Println("Plaintext " + host)
	_, msg = plainText(host)
	if processResults(host, "plaintext", msg, tag) {
		fmt.Println(host, " End Time: ", time.Now().Format(time.RFC850))
		return
	}
	fmt.Println("SASL-Plaintext " + host)
	_, msg = saslPlain(host)
	if processResults(host, "sasl_plaintext", msg, tag) {
		fmt.Println(host, " End Time: ", time.Now().Format(time.RFC850))
		return
	}
	fmt.Println("SASL-SSL " + host)
	_, msg = saslTLS(host)
	if processResults(host, "sasl_ssl", msg, tag) {
		fmt.Println(host, " End Time: ", time.Now().Format(time.RFC850))
		return
	}
	fmt.Println("TLS " + host)
	_, msg = tlsClient(host)
	processResults(host, "tls", msg, tag)
	fmt.Println(host, " End Time: ", time.Now().Format(time.RFC850))
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

func processResults(host string, securityType string, msg string, tags string) bool {
	if strings.Contains(msg, "ConnectionSuccessStart") {
		msg = strings.ReplaceAll(msg, "ConnectionSuccessStart:", "")
		msg = strings.ReplaceAll(msg, "ConnectionSuccessEnd", "")
		saveResults(host, securityType, msg, "", tags)
		return true
	} else if strings.Contains(msg, "Failed to connect to broker") {
		// not doing anything
		fmt.Println("Failed to connect to broker")
		return false
	} else {
		msg = strings.ReplaceAll(msg, "ConsumerStart:", "")
		msg = strings.ReplaceAll(msg, "ConsumerEnd", "")
		saveResults(host, securityType, "", msg, tags)
		return false
	}
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

var (
	host     = os.Getenv("PGHOST")
	port     = 25061
	user     = os.Getenv("PGUSER")
	password = os.Getenv("PGPASSWORD")
	dbname   = "internetmeasurements" // "defaultdb"
)

var db *sql.DB
var err error

func openDb() {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require binary_parameters=yes", host, port, user, password, dbname)

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
	if success != "" && len(success) > 19000 {
		success = success[0:1900]
	}
	_, e := db.Exec(insertDynStmt, broker, security_method, success, errors, tags)
	if e != nil {
		fmt.Println("Error occurred writing to db: ", e)
		// time.Sleep(8 * time.Second)
	}
}

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

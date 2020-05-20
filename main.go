package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	aero "github.com/aerospike/aerospike-client-go"
)

const (
	// csv-realted
	namespace = "test"
	setName   = "creditcard"

	// BinMap field names
	setNameBin = "set_name"
	userIDBin  = "ID"
	amountCol  = "AmountBin"
	classBin   = "ClassBin"
	timeBin    = "TimeBin"

	localhost         = "127.0.0.1"
	mlModelServingURL = "http://" + localhost + ":8501/v1/models/fraud:predict"
	inputLength       = 29
	fraudThreshold    = 0.5
)

var aeroClient *aero.Client

// webTxn is a struct for txn incoming in a web request
type webTxn struct {
	Timestamp string
	Amount    float64
	UserID    string
	SellerID  string
	ItemID    string
}

// enrichedTxn is a struct for sending to the ML model
type enrichedTxn struct {
	Inputs [1][inputLength]float64 `json:"inputs"`
}

// prediction is a struct for gettingt the prediction from the ML model
type prediction struct {
	Value [1][1]float64 `json:"outputs"`
}

// predictionHandler is the entry point to the system,
// ends in validating the prediction
func predictionHandler(w http.ResponseWriter, req *http.Request) {
	// read t§xn, decode JSON, store in Aerospike
	incomingTxn := webTxn{}
	err := acceptTxn(req, aeroClient, &incomingTxn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// read txn by userID
	enrichedTxn := enrichedTxn{}
	txnOutcome, err := enrichTxn(aeroClient, &incomingTxn, &enrichedTxn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// send enriched txn to model serving web service
	modelPrediction, err := getPrediction(&enrichedTxn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// compare prediction with classification
	validatePrediction(txnOutcome, modelPrediction)
}

// acceptTxn reads an incoming txn and stores it in Aerospike
func acceptTxn(req *http.Request, client *aero.Client, incomingTxn *webTxn) (err error) {
	if err := json.NewDecoder(req.Body).Decode(&incomingTxn); err != nil {
		return err
	}
	key, err := aero.NewKey(namespace, setName, incomingTxn.SellerID)
	if err != nil {
		return err
	}
	ibm := aero.BinMap{
		userIDBin:  incomingTxn.UserID,
		setNameBin: setName,
		amountCol:  incomingTxn.Amount,
	}
	err = client.Put(nil, key, ibm)
	// store the incoming txn  in Aerospike
	return nil
}

// enrichTxn creates the enriched txn based on the given UserID
func enrichTxn(client *aero.Client, incomingTxn *webTxn, enrichedTxn *enrichedTxn) (txnOutcome string, err error) {

	key, err := aero.NewKey(namespace, setName, incomingTxn.UserID)
	if err != nil {
		return "", err
	}
	r, err := client.Get(nil, key, classBin)
	if err != nil {
		return "", err
	}

	pca := [inputLength]float64{}
	i := 0
	for k, v := range r.Bins {
		if k == timeBin {
			continue
		}
		pcaV, ok := v.(float64)
		if !ok {
			pcaV = 0
		}
		if k == amountCol {
			pcaV = math.Log(pcaV)
		}
		if k == classBin {
			txnOutcome = strconv.FormatFloat(pcaV, 'f', -1, 64)
			continue
		}
		pca[i] = pcaV
		i++
	}
	enrichedTxn.Inputs[0] = pca
	return txnOutcome, err
}

// getPrediction sends the enriched txn to the model and gets prediction
func getPrediction(enrichedTxn *enrichedTxn) (modelPrediction string, err error) {
	// prepare the request to the web service serving the model
	reqBody, err := json.Marshal(enrichedTxn)
	if err != nil {
		return "", err
	}
	// make the request
	req, err := http.NewRequest(http.MethodPost, mlModelServingURL, bytes.NewBuffer(reqBody))
	req.Header.Set("Content-Type", "application/json")
	// read the response
	client := &http.Client{
		Timeout: time.Second * 10,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	prediction := prediction{}
	err = json.Unmarshal(data, &prediction)
	if err != nil {
		return
	}
	if prediction.Value[0][0] > fraudThreshold {
		fmt.Println("Prediction is FRAUD")
		modelPrediction = "1"
	} else {
		fmt.Println("Prediction is NOT FRAUD")
	}
	return modelPrediction, nil
}

// validatePrediction compares the model prediction with the classification from the DB
func validatePrediction(txnOutcome, modelPrediction string) {
	if txnOutcome == modelPrediction {

	}
	// compare both predictions

	// advanced: run comparison for all fields in csv
	return
}

func main() {
	// set up a single instance of an Aerospike client
	// connection, it handles the connection pool internally
	var err error
	aeroClient, err = aero.NewClient(localhost, 3000)
	if err != nil {
		log.Fatal(err)
	}

	// listen and serve
	http.HandleFunc("/", predictionHandler)
	http.ListenAndServe(":8090", nil)
}

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type CredentialsFile struct {
	Credentials string `json:"Credentials"`
}

type Credentials struct {
	Endpoint    string `json:"endpoint"`
	AccessKeyId string `json:"accessKeyId"`
	SecretKeyId string `json:"secretKeyId"`
	Bucket      string `json:"bucket"`
}

func main() {
	path := os.Getenv("CREDENTIALS_FILE_PATH")
	log.Printf("#%v ", path)

	file := &CredentialsFile{}
	jsonFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("Path - %s, jsonFile.Get err   #%v ", path, err)
	}
	err = json.Unmarshal(jsonFile, file)
	if err != nil {
		log.Fatalf("Unmarshal: %+v", err)
	}

	creds := &Credentials{}
	err = json.Unmarshal([]byte(file.Credentials), creds)
	if err != nil {
		log.Fatalf("Unmarshal: %+v", err)
	}

	err = os.WriteFile("/tmp/bucket", []byte(creds.Bucket), 0644)
	if err != nil {
		log.Fatalf("os.WriteFile: %+v", err)
	}
}

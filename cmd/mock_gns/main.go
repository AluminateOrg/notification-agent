package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type registerReq struct {
	Token string `json:"token"`
}

type registerResp struct {
	OrgId string `json:"id"`
	Name string `json:"name"`
}

const expectedBoostrapToken = "sample_bootstrap_token_123"

func registerHandler(w http.ResponseWriter, r *http.Request) {
	var body registerReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if body.Token != expectedBoostrapToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	time.Sleep(200 * time.Millisecond)
	resp := registerResp{
		OrgId: "orgId-" + uuid.NewString(),
		Name: "Org",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	http.HandleFunc("/register", registerHandler)
	log.Println("Mock GNS listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}